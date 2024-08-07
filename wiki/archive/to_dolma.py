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

import pandas as pd
import pytz

from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma
from licensed_pile.xml import iterate_xmls

parser = argparse.ArgumentParser(
    description="Convert Downloaded Wiki dumps from the internet archive to dolma."
)
parser.add_argument("--wiki_metadata", default="data/ia-wikis.jsonl")
parser.add_argument("--dump_dir", help="The location of the IA dump.")
parser.add_argument(
    "--output_dir",
    help="Where the dolma formatted data goes.",
)
parser.add_argument("--filename", help="The base filename for our wiki data.")
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)
parser.add_argument("--processes", type=int, default=4, help="")
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
    page,
    source_name: str,
    wiki: str,
    dump_url: str,
    url: str,
    license: PermissiveLicenses,
):
    """Convert old style wiki dumps with a pages/ dir into the dolma format."""
    with open(f"{page}.wikitext") as f:
        text = f.read()
    metadata = pd.read_csv(f"{page}.history.csv")
    metadata = metadata.replace(math.nan, None)
    authors = set(metadata["Author"])
    # Use .discard instead of .remove in case None isn't in the author set.
    authors.discard(None)

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
    if skip_redirect and [x for x in xml if x.tag.endswith("redirect")]:
        return None
    revisions = [r for r in xml if r.tag.endswith("revision")]
    # TODO Handle if this fails and add logging.
    text = [t for t in revisions[-1] if t.tag.endswith("text")][0].text
    page_namespace = [ns for ns in xml if ns.tag.endswith("ns")][0].text
    page_id = [pid for pid in xml if pid.tag.endswith("id")][0].text
    created = datetime.datetime.fromisoformat(
        [ts for ts in revisions[-1] if ts.tag.endswith("timestamp")][0].text
    ).replace(tzinfo=None)
    page_title = [t for t in xml if t.tag.endswith("title")][0].text

    contributors = set()
    if all_authors:
        for revision in revisions:
            contribs = [c for c in revision if c.tag.endswith("contributor")]
            # When there are multiple contributors, there are multiple contributor
            # xml items where each one has a single username and id items.
            names = [u.text for c in contribs for u in c if u.tag.endswith("username")]
            # Save their id too in case they change their username
            uid = [u.text for c in contribs for u in c if u.tag.endswith("id")]
            contributors.update(zip(names, uid))
    else:
        contrib = [c for c in revisions[-1] if c.tag.endswith("contributor")]
        # When there are multiple contributors, there are multiple contributor
        # xml items where each one has a single username and id items.
        name = [u.text for c in contrib for u in c if u.tag.endswith("username")]
        # Save their id too in case they change their username
        uid = [u.text for c in contrib for u in c if u.tag.endswith("id")]
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


# TODO: Add a check if the converted one is already there, skip unless overwrite
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
    logger = logs.get_logger("wiki/archive")
    if "metadata" not in wiki:
        logger.error(f"Metadata missing from line, malformed record")
        return None
    ident = wiki["metadata"]["identifier"]
    wiki_path = os.path.join(dump_dir, ident)
    if not os.path.exists(wiki_path):
        # logger.warning(f"Dump for wiki {ident} is missing from {wiki_path}")
        return None
    logger.info(f"Converting wiki: {ident} to dolma.")
    if filename is None:
        filename = f"{ident}.jsonl.gz"
        filename = "wiki.jsonl.gz"
    output_dir = os.path.join(output_dir, ident, "documents")
    logger.info(f"Writing Dolma documents to {output_dir}")

    pages = os.path.join(wiki_path, "pages")
    if os.path.exists(pages) and os.path.isdir(pages):
        logger.info(f"Wiki: {ident} is an old-style dump.")
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
        export_pages = glob.glob(os.path.join(wiki_path, "*-history.xml"))
        if not export_pages:
            logger.error(f"Can't find *-histroy.xml file for wiki: {ident}")
            return None
        logger.info(f"Wiki: {ident} is a new-style dump.")
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
    to_dolma(pages, output_dir, filename, shard_size)


def main(args):
    logger = logs.get_logger("wiki/archive")

    logger.info(f"Reading wiki metadata from {args.wiki_metadata}")
    with open(args.wiki_metadata) as f:
        wiki_metadata = [json.loads(l) for l in f if l]
    logger.info(f"{len(wiki_metadata)} wikis to convert.")

    args.dump_dir = (
        args.dump_dir
        if args.dump_dir is not None
        else os.path.dirname(args.wiki_metadata)
    )
    args.output_dir = (
        args.output_dir
        if args.output_dir is not None
        else os.path.join("data", "wiki/archive", "raw")
    )

    # with mp.Pool(args.processes) as pool:
    #     pool.map(
    list(
        map(
            functools.partial(
                convert_wiki,
                source_name="wiki/archive",
                dump_dir=args.dump_dir,
                output_dir=args.output_dir,
                filename=args.filename,
                shard_size=args.shard_size,
                all_authors=not args.last_author,
                skip_redirect=not args.include_redirects,
            ),
            wiki_metadata,
        )
    )


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("wiki/archive")
    main(args)
