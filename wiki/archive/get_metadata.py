"""Download wiki dump metadata from the internet archive.

The licenseurl regex's we are using to search are mutually exclusive so we can
split the query into multiple chunks instead of `OR`ing them together to get some
parallelism out of the metadata scrape. Downloading the metadata for the 350k wikis
in a single query to the IA API took hours.

TODO: If we include the "wikicollections" data, (wikis uploaded to the IA that
aren't scraped by the wikiteam) we jump up to ~4million wikis compared to the
350k we get from wikiteam.
"""

import argparse
import json
import multiprocessing.dummy as mp
import os
import re
import shutil

import internetarchive

from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses

parser = argparse.ArgumentParser(
    description="Download metadata for wiki dumps from the IA."
)
parser.add_argument("--output_dir", default="data/metadata/", help="")
parser.add_argument("--file_name", default="ia-wiki-metadata.jsonl")
# TODO: Respect these
parser.add_argument("--include_wikicollections", action="store_true", help="")
parser.add_argument("--licenses", choices=[], action="append", help="")


def get_metadata(idx: int, query: str, file_name: str, output_dir: str):
    """Fetch item metadata from IA using query and save it to disk."""
    logger = logs.get_logger()
    logger.info(f"Querying IA with {query}")
    with open(os.path.join(output_dir, f"{idx:>05}_{file_name}"), "w") as wf:
        # This is a cursor so it fetches items from the IA as we
        # iterate over it.
        for item in internetarchive.search_items(query):
            wf.write(json.dumps(i.item_metadata) + "\n")


def make_queries(licenses, include_wikicollections):
    """Convert the CLI args into a collection of queries to make."""
    if include_wikicollections:
        raise NotImplementedError("...")
    license_regexs = licenses
    for license_regex in license_regexs:
        yield f"collection:(wikiteam) AND licenseurl:({license_regex})"


def merge_shards(output_dir, file_name):
    """Merge each of our response shards into one."""
    shards = []
    for fname in os.listdir(output_dir):
        if m := re.match(rf"(\d{{5}}_{file_name})"):
            shards.append(m.group(1))
    logger = logs.get_logger()
    shards = sorted(shards)
    logger.info(f"Found {len(shards)} shards, {shards}")
    with open(os.path.join(output_dir, file_name), "wb") as wf:
        for in_file in shards:
            with open(os.path.join(output_dir, in_file), "rb") as f:
                # Use shutil to copy the file in chunks without the overhead
                # of acutally finding line endings required by `for line in f`
                # or .readlines()
                # Note: We know that each shard file ends with a newline so we
                # can just concatenate them, we don't need to insert another
                # newline.
                shutil.copyfileobj(f, wf)


def main(args):
    # TODO have something that translates from the PermissiveLicense Enum to regex's
    if args.licenses is None:
        args.licesnes = (
            "*\/by\/*",
            "*\/by-sa\/*",
            "*publicdomain*",
            "*GNU_Free_Documentation_License*",
        )
    queries = list(make_queries(args.licesnses, args.include_wikicollections))
    with mp.Pool(len(queries)) as pool:
        pool.starmap(
            functools.partial(
                get_metadata, file_name=args.file_name, output_dir=args.output_dir
            ),
            enumerate(queries),
        )
    merge_shards(args.output_dir, args.file_name)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging()
    main(args)
