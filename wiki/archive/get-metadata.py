"""Download wiki dump metadata from the internet archive.

The licenseurl regex's we are using to search are mutually exclusive so we can
split the query into multiple chunks instead of `OR`ing them together to get some
parallelism out of the metadata scrape.
"""

import argparse
import json
import multiprocessing.dummy as mp
import os

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
    with open(os.path.join(output_dir, f"{idx:>05}_{file_name}")) as wf:
        for item in internetarchive.search_items(query):
            wf.write(json.dumps(i.item_metadata) + "\n")


def make_queries(licenses, include_wikicollections):
    if include_wikicollections:
        raise NotImplementedError("...")
    license_regexs = licenses
    for license_regex in license_regexs:
        yield f"collection:(wikiteam) AND licenseurl:({license_regex})"


def main(args):
    # TODO have something that translates from the PermissiveLicense Enum to regex's
    if args.licenses is None:
        args.licesnes = [
            "*\/by\/*",
            "*\/by-sa\/*",
            "*publicdomain*",
            "*GNU_Free_Documentation_License*",
        ]
    queries = list(make_queries(args.licesnses, args.include_wikicollections))
    with mp.Pool(len(queries)) as pool:
        pool.starmap(
            functools.partial(
                get_metadata, file_name=args.file_name, output_dir=args.output_dir
            ),
            enumerate(queries),
        )


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("wiki/archive")
    main(args)
