"""Convert Arxiv Dumps into the dolma format."""

import argparse
import copy
import datetime
import functools
import gzip
import itertools
import json
import os
import re
import tarfile
from typing import Dict, Optional, Sequence, Set, Tuple

from bulk_download import BulkDownloader
from charset_normalizer import from_bytes

from common_pile import logs
from common_pile.licenses import PermissiveLicenses
from common_pile.write import to_dolma

SOURCE_NAME = "arxiv"

parser = argparse.ArgumentParser(description="Convert Arxiv dumps to the dolma format.")
parser.add_argument(
    "--metadata",
    default="data/arxiv-metadata-oai-snapshot.json",
    help="Path to the arxiv metadata file.",
)
parser.add_argument(
    "--dump_dir", default="data/src", help="Path to the arxiv src dump."
)
parser.add_argument(
    "--output_dir",
    default=f"data/{SOURCE_NAME}/raw/documents/",
    help="Where the dolma formatted data goes.",
)
parser.add_argument(
    "--filename", default="arxiv.jsonl.gz", help="The base filename for our chat data."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)
parser.add_argument(
    "--manifest",
    default="data/arXiv_src_manifest.xml",
    help="The manifest file for the arxiv source dump.",
)
parser.add_argument(
    "--dry_run", action="store_true", help="Should we not actually download any files."
)


LICENSES = {
    "http://creativecommons.org/licenses/by-sa/4.0/": PermissiveLicenses.CC_BY_SA,
    "http://creativecommons.org/licenses/by/3.0/": PermissiveLicenses.CC_BY_3,
    "http://creativecommons.org/licenses/by/4.0/": PermissiveLicenses.CC_BY,
    "http://creativecommons.org/licenses/publicdomain/": PermissiveLicenses.PD,
    "http://creativecommons.org/publicdomain/zero/1.0/": PermissiveLicenses.CC0,
}


def id_to_directory(arxiv_id: str) -> str:
    """Arxiv is split into directories based on the first 4 digits of the id.

    Some ids have non-numerical parts to it, e.g. nucl-th/9212001
    """
    if m := re.search(r"([0-9]{4})", arxiv_id):
        return m.group(1)
    return None


def id_to_filename(arxiv_id: str) -> str:
    """Id's that have slashes (non-numerical ones like math9212204) get the slashed removed in the filename."""
    return arxiv_id.replace("/", "")


def skip_file(filename: str, to_skip: Set[str]) -> bool:
    r"""Check if filename.ext or just filename is in to_skip.

    Note:
      Sometimes people use \input{filename} or \input{filename.tex}
      to include other files in latex.
    """
    return filename in to_skip or os.path.splitext(filename)[0] in to_skip


def read_file(tar, file_info, article_id: str):
    try:
        contents = tar.extractfile(file_info).read()
        if isinstance(contents, bytes):
            contents = str(from_bytes(contents).best())
        return contents
    except Exception as e:
        logger = logs.get_logger("arxiv")
        logger.warning(
            f"Failed to read {file_info.filename} for article [{article_id}]: {e}"
        )
        return ""


def interpolate_document(
    contents: str, tar, skip: Set[str], article_id: str
) -> Tuple[str, Set[str]]:
    full_contents = []
    offset = 0
    logger = logs.get_logger("arxiv")

    # Capture the whole command so we know the bounds and can remove it.
    for m in re.finditer(r"(?P<cmd>\\input{(?P<filename>.*?)})", contents):
        # Add everything before the \input command.
        full_contents.append(contents[offset : m.start()])
        offset = m.end()
        # Add everything from the file used in the \input command.
        # TODO: Add support for nested \input? Pathing gets weird when you nest
        # so there is a package called import which solve it. We might want to
        # skip this as it seems complex. Unsure how many documents include the
        # \import or \subimport command
        try:
            input_file = tar.getmember(
                f"{os.path.splitext(m.group('filename'))[0]}.tex"
            )
            input_contents = read_file(tar, input_file, article_id)
            logger.debug(f"Interpolating {m.group('filename')} into {article_id}")
            full_contents.append(input_contents)
            # Track which files we have interpolated into other documents to avoid
            # using them as top-level documents.
            skip.add(m.group("filename"))
        except Exception as e:
            logger.warning(
                f"Failed to interpolate {m.group('filename')} while processing [{article_id}]: {e}"
            )
            # If we fail to read the interpolated data from the `\input`, just
            # stick the raw latex back in there.
            full_contents.append(contents[m.start() : m.end()])
    # Include everything from the end of the final match (or the beginning if no
    # \input was used) until the end.
    full_contents.append(contents[offset:])
    return "".join(full_contents), skip


def load_article_src(article_path: str, article_id: str) -> str:
    """article_path should be a `.gz` file."""
    logger = logs.get_logger("arxiv")
    if not os.path.exists(article_path):
        logger.warning(f"Article `{article_id}` source is missing.")
        if not os.path.exists(f"{os.path.splitext(article_path)[0]}.pdf"):
            logger.warning(f"Article `{article_id}` pdf is missing too.")
        return []
    try:
        # If the src is a tar file, then we will emit one document for each
        # top-level .tex file.
        if tarfile.is_tarfile(article_path):
            skip = set()
            with tarfile.open(article_path, "r:gz") as tar:
                for info in tar:
                    # If we interpolated this file into another, don't use it as
                    # a document.
                    if skip_file(info.name, skip):
                        continue
                    # If the file is a .tex document and it isn't in a directory.
                    if (
                        os.path.splitext(info.name)[1] == ".tex"
                        and os.path.dirname(info.name) == ""
                    ):
                        logger.debug(
                            f"Creating a document from {article_id}/{info.name}t"
                        )
                        contents = read_file(tar, info, article_id)
                        # Only output files that include \begin{document}. If
                        # they don't they are probably latex fragments.
                        if r"\begin{document}" in contents:
                            content, skip = interpolate_document(
                                contents, tar, skip, article_id
                            )
                            yield content, info.name
        # If it isn't a tarfile, it is at least gzip compressed.
        else:
            logger.debug(f"Creating a document from single file for {article_id}")
            with gzip.open(article_path) as f:
                contents = f.read()
            # If the file isn't a tarball, then it won't have any extra files
            # that are `\input`ed.
            yield content, None
    except Exception as e:
        logger.warning(
            f"Failed to load article [{article_id}] at `{article_path}`: {e}"
        )
        return []


def format_dolma(article, text: str, source: str = SOURCE_NAME):
    return {
        "id": article["id"],
        "text": text,
        "source": "arxiv",
        "added": datetime.datetime.utcnow().isoformat(),
        "created": article["update_date"],
        "metadata": {
            "license": str(LICENSES.get(article["license"], article["license"])),
            "url": f"http://arxiv.org/abs/{article['id']}",
            "authors": article["authors"],
            "title": article["title"],
        },
    }


def process_article(
    article, dump_dir: str, bulk_downloader: BulkDownloader
) -> Sequence[Tuple[Dict, str]]:
    logger = logs.get_logger("arxiv")
    # The bulk downloader is smart enough to only download dirs when it needs
    # it so we can just call `.download` on all articles and know that it
    # won't keep re-downloading things.
    # The start and end fields of the shards use the same verions of ids as
    # filenames, that is, without the `/`
    bulk_downloader.download(id_to_filename(article["id"]))
    article_path = os.path.join(
        dump_dir,
        id_to_directory(article["id"]),
        f"{id_to_filename(article['id'])}.gz",
    )
    # We can emit multiple documents if there are multiple top-level .tex files.
    # If this collection of all documents produced by a single article id has
    # only one document, don't include the filename in the dolma id.
    contents_and_filename = list(load_article_src(article_path, article["id"]))
    logger.debug(
        f"Article {article['id']} generated {len(contents_and_filename)} documents."
    )
    if not contents_and_filename:
        return []
    if len(contents_and_filename) > 1:
        for contents, filename in contents_and_filename:
            updated_article = copy.deepcopy(article)
            updated_article["id"] = f"{updated_article['id']}-{filename}"
            yield updated_article, contents
    else:
        yield article, contents_and_filename[0][0]


def main(args):
    with open(args.metadata) as f:
        metadata = [json.loads(l) for l in f]

    bulk_downloader = BulkDownloader(
        args.manifest,
        # Having to grab the dir for this is ugly, but it is a quirk of the
        # shards having their own `src/` dir in their names. Not worth cleaning
        # up.
        output_dir=os.path.dirname(args.dump_dir),
        overwrite=False,
        dry_run=args.dry_run,
    )

    # Use iterators so we don't load the whole dataset into memory.
    cc_articles = (a for a in metadata if a["license"] in LICENSES)
    process = functools.partial(
        process_article, dump_dir=args.dump_dir, bulk_downloader=bulk_downloader
    )
    meta_and_content = itertools.chain(*map(process, cc_articles))
    dolma = map(lambda x: format_dolma(*x), meta_and_content)
    to_dolma(dolma, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("arxiv")
    main(args)
