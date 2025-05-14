"""Convert wiki dumps from the internet archive to dolma."""

import argparse
import datetime
import functools
import glob
import json
import math
import multiprocessing as mp
import os
import re
import uuid

import pandas as pd
import pytz
import utils

from common_pile import logs
from common_pile.licenses import PermissiveLicenses
from common_pile.utils import dolma_output
from common_pile.write import to_dolma
from common_pile.xml import iterate_xmls

parser = argparse.ArgumentParser(
    description="Convert Downloaded Wiki dumps from the internet archive to dolma."
)
parser.add_argument("--wiki_metadata", default="data/wiki/archive/ia-wikis.jsonl")
parser.add_argument(
    "--dump_dir", help="The location of the IA dump.", default="data/wiki/archive/dumps"
)
parser.add_argument(
    "--output_dir",
    help="Where the dolma formatted data goes.",
)
parser.add_argument("--filename", help="The base filename for our wiki data.")
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)
parser.add_argument(
    "--last_author",
    action="store_true",
    help="Should we only include the most recent author? (Faster)",
)
parser.add_argument(
    "--include_redirects",
    action="store_true",
    help="Should we skip pages that are redirects to others?",
)


def format_old(
    page: str,
    source_name: str,
    wiki: str,
    dump_url: str,
    url: str,
    license: PermissiveLicenses,
):
    """Convert old style wiki dumps with a pages/ dir into the dolma format."""
    logger = logs.get_logger()
    try:
        with open(f"{page}.wikitext") as f:
            text = f.read()
        metadata = pd.read_csv(f"{page}.history.csv")
        metadata = metadata.replace(math.nan, None)
        authors = set(metadata["Author"])
        # Use .discard instead of .remove in case None isn't in the author set.
        authors.discard(None)
        # Update the author format to match the [(name, id), ...] format the new .xml has.
        authors = [(str(a), "") for a in authors]

        # The date column is formatted as "Date (timezone)". Here we find which timezone
        # it is. Unclear if the tz is the same for all dumps so we infer it here.
        tz = [
            tz.group("tz")
            for col in metadata.columns
            if (tz := re.match(r"^Date \((?P<tz>.*?)\)$", col))
        ]
        tz = tz[0] if tz else "UTC"
        date_col = f"Date ({tz})"
        tz = pytz.timezone(tz)

        # We are going to use the most recent revision so the "created" for that
        # version if the most recent date.
        dates = metadata[date_col].apply(
            lambda d: datetime.datetime.strptime(d, "%Y-%m-%d %H:%M:%S")
        )
        created = max(dates)

        page_title = os.path.basename(page)

        return {
            "id": page_title,
            "text": text,
            "source": f"{source_name}/{wiki}",
            "added": datetime.datetime.utcnow().isoformat(),
            "created": created.isoformat(),
            "metadata": {
                "license": str(license),
                "authors": sorted(authors),
                # Things dumped with only the old format are generally not online
                # anymore so use the dump url.
                "dump_url": dump_url,
                "wiki": wiki,
                "url": url,
                # These old dumps don't include namespaces.
                "namespace": None,
                "title": page_title,
            },
        }
    except:
        logger.exception(f"Failed to parse {wiki}:{page}")


def format_xml(
    xml,
    source_name: str,
    wiki: str,
    url: str,
    dump_url: str,
    license: PermissiveLicenses,
    all_authors: bool = True,
    skip_redirect: bool = True,
):
    """Convert a -history.xml file to the dolma format."""
    # TODO: This is shared with the generic dolma version, but more robust, should be unified.
    logger = logs.get_logger()
    if skip_redirect and [x for x in xml if x.tag.endswith("redirect")]:
        # Don't log this as we haven't extracted any information to make the log
        # entry useful.
        # logger.info("Skipping page as it is a redirect.")
        return None

    revisions = [r for r in xml if r.tag.endswith("revision")]
    if not revisions:
        logger.error(f"Failed to parse revision for page", extra={"wiki": wiki})
        return None
    text = [t for t in revisions[-1] if t.tag.endswith("text")]
    if not text:
        logger.error(f"Failed to parse page text", extra={"wiki": wiki})
        text = None
    else:
        text = text[0].text

    page_namespace = [ns for ns in xml if ns.tag.endswith("ns")]
    if not page_namespace:
        page_namespace = ""
        logger.warning(f"Failed to parse namespace", extra={"wiki": wiki})
    else:
        page_namespace = page_namespace[0].text

    page_id = [pid for pid in xml if pid.tag.endswith("id")]
    if not page_id:
        logger.warning(f"Filed to find page id, generating uuid", extra={"wiki": wiki})
        page_id = uuid.uuid4()
    else:
        page_id = page_id[0].text

    ts = [ts for ts in revisions[-1] if ts.tag.endswith("timestamp")]
    if not ts:
        logger.warning("Failed to parse timestamp, using default", extra={"wiki": wiki})
        ts = "1970-01-01"
    else:
        ts = ts[0].text
    try:
        created = datetime.datetime.fromisoformat(ts).replace(tzinfo=None)
    except TypeError:
        logger.warning(
            f"Failed to parse timestamp: {ts}", extra={"wiki": wiki}, exc_info=True
        )
        created = datetime.datetime.fromisoformat("1970-01-01").replace(tzinfo=None)

    page_title = [t for t in xml if t.tag.endswith("title")]
    if not page_title:
        logger.warning(f"Failed to parse page title", extra={"wiki": wiki})
        page_title = ""
    else:
        page_title = page_title[0].text

    contributors = set()
    if all_authors:
        for revision in revisions:
            contribs = [c for c in revision if c.tag.endswith("contributor")]
            # When there are multiple contributors, there are multiple contributor
            # xml items where each one has a single username and id items.
            names = [u.text for c in contribs for u in c if u.tag.endswith("username")]
            name = ["" if n is None else n for n in name]
            # Save their id too in case they change their username
            uid = [u.text for c in contribs for u in c if u.tag.endswith("id")]
            uid = ["" if u is None else u for u in uid]
            contributors.update(zip(names, uid))
    else:
        # We already checked if revisions was empty above, so we will always
        # have a revisions[-1] to check.
        contrib = [c for c in revisions[-1] if c.tag.endswith("contributor")]
        # When there are multiple contributors, there are multiple contributor
        # xml items where each one has a single username and id items.
        name = [u.text for c in contrib for u in c if u.tag.endswith("username")]
        name = ["" if n is None else n for n in name]
        # Save their id too in case they change their username
        uid = [u.text for c in contrib for u in c if u.tag.endswith("id")]
        uid = ["" if u is None else u for u in uid]
        contributors.update(zip(name, uid))

    return {
        "id": f"{page_namespace}-{page_id}",
        "text": text,
        "source": f"{source_name}/{wiki}",
        "added": datetime.datetime.utcnow().isoformat(),
        "created": created.isoformat(),
        "metadata": {
            "license": str(license),
            "authors": sorted(contributors),
            "url": url,
            "wiki": wiki,
            "dump_url": dump_url,
            "namespace": page_namespace,
            "title": page_title,
        },
    }


def convert_wiki(
    wiki,
    source_name: str,
    dump_dir: str,
    output_dir: str,
    filename: str,
    shard_size: int,
    all_authors: bool = True,
    skip_redirect: bool = True,
):
    """Convert a wiki into the dolma format, support new and old style wikis."""
    logger = logs.get_logger()
    if "metadata" not in wiki:
        logger.error(f"Metadata missing from line, malformed record")
        return None
    ident = wiki["metadata"]["identifier"]
    wiki_path = os.path.join(dump_dir, utils.wiki_to_dir(ident))
    if not os.path.exists(wiki_path):
        logger.warning(f"Dump for wiki {ident} is missing from {wiki_path}")
        return None
    logger.info(f"Converting wiki: {ident} to dolma.")
    filename = "wiki.jsonl.gz" if filename is None else filename
    dolma_dir = os.path.join(output_dir, utils.wiki_to_dir(ident))
    # Use a shadow dir to allow for starting and stopping in the middle of
    # conversion. This lets us skip processing wikis that already have an
    # output without worrying that the output is incomplete.
    shadow_dir = os.path.join(output_dir, utils.wiki_to_dir(ident), "shadow")
    os.makedirs(shadow_dir, exist_ok=True)
    if os.path.exists(dolma_dir):
        logger.warning(f"{dolma_dir} already exists, skipping")
        return
    logger.info(f"Writing Dolma documents to {shadow_dir}, shadowing {dolma_dir}")

    # Checking for an old style wiki.
    pages = os.path.join(wiki_path, "pages")
    if os.path.exists(pages) and os.path.isdir(pages):
        logger.info(f"Wiki: {ident} is an old-style dump.")
        # Get all the wikitext files via glob, but then remove them as
        # we use the basename to access wikitext and history later.
        pages = glob.iglob(os.path.join(pages, "*.wikitext"))
        pages = map(lambda p: os.path.splitext(p)[0], pages)
        pages = map(
            functools.partial(
                format_old,
                source_name=source_name,
                wiki=ident,
                dump_url=wiki["metadata"].get("identifier-access"),
                url=wiki["metadata"].get("originalurl"),
                license=PermissiveLicenses.from_string(wiki["metadata"]["licenseurl"]),
            ),
            pages,
        )
    else:
        logger.info(f"Wiki: {ident} is a new-style dump.")
        export_pages = glob.glob(os.path.join(wiki_path, "*-history.xml"))
        if not export_pages:
            logger.error(f"Can't find *-histroy.xml file for wiki: {ident}")
            return None
        pages = iterate_xmls(export_pages, tag="page")
        pages = map(
            functools.partial(
                format_xml,
                source_name=source_name,
                wiki=ident,
                dump_url=wiki["metadata"].get("identifier-access"),
                url=wiki["metadata"].get("originalurl"),
                license=PermissiveLicenses.from_string(wiki["metadata"]["licenseurl"]),
                all_authors=all_authors,
                skip_redirect=skip_redirect,
            ),
            pages,
        )
    # Wiki processing is all via iterators so we don't have memory issues.
    pages = filter(lambda p: p is not None, pages)
    to_dolma(pages, shadow_dir, filename, shard_size)
    # Move the shadow page to the real output location
    try:
        os.makedirs(os.path.dirname(dolma_dir), exist_ok=True)
        os.rename(shadow_dir, dolma_dir)
        logger.info(
            f"Dolma conversion for {ident} complete, moving shadow from {shadow_dir} to {dolma_dir}"
        )
    # If something goes wrong moving the shadow page, delete the output
    # dir as it will be incomplete, but its presense would cause this
    # wiki to be skipped when resuming processing.
    except Exception:
        os.remove(os.path.dirname(dolma_dir))
    finally:
        os.rmdir(os.path.dirname(shadow_dir))


def main(args):
    logger = logs.get_logger()

    logger.info(f"Reading wiki metadata from {args.wiki_metadata}")
    with open(args.wiki_metadata) as f:
        wiki_metadata = [json.loads(l) for l in f if l]
    logger.info(f"{len(wiki_metadata)} wikis to convert.")

    args.dump_dir = (
        args.dump_dir
        if args.dump_dir is not None
        else os.path.dirname(args.wiki_metadata)
    )
    args.output_dir = dolma_output(
        args.output_dir
        if args.output_dir is not None
        else os.path.join("..", "data", "wiki", "archive", "raw")
    )

    convert = functools.partial(
        convert_wiki,
        source_name="wiki/archive",
        dump_dir=args.dump_dir,
        output_dir=args.output_dir,
        filename=args.filename,
        shard_size=args.shard_size,
        all_authors=not args.last_author,
        skip_redirect=not args.include_redirects,
    )

    # Run the action convert function, without a for loop.
    # Note: I looked at using mp.Pool here, but there is so much disk IO
    # that I was seeing much slower speeds than doing it serially.
    list(map(convert, wiki_metadata))


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging()
    main(args)
