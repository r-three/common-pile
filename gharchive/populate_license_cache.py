"""Populate the license cache from our data."""

import argparse
import glob
import itertools
import json
import shelve

from ghapi.all import GhApi
from to_dolma import LicenseInfo, LicenseSnapshot, get_license_info, read_threads

from licensed_pile import logs

parser = argparse.ArgumentParser(description="Convert threads to dolma.")
parser.add_argument(
    "--bq", help="Pattern that picks up to the bigquery exported files."
)
parser.add_argument(
    "--repo_list", help="Path to a file that has a list of repos (JSON)."
)
parser.add_argument(
    "--license_cache", default="data/license_cache", help="Where to cache license data."
)


def main():
    args = parser.parse_args()
    api = GhApi()
    logger = logs.configure_logging(level="INFO")
    if args.bq:
        logger.info(f"Reading data from shards according to {args.bq}")
        files = glob.iglob(args.bq)
        threads = itertools.chain(*map(read_threads, files))
    elif args.repo_list:
        with open(args.repo_list) as f:
            repos = json.load(f)
        threads = [{"repo_name": r for r in repos}]
    else:
        raise ValueError("Either --bq or --repo_list must be provided.")
    with shelve.open(args.license_cache) as license_cache:
        for thread in threads:
            _ = get_license_info(thread["repo_name"], license_cache, api)


if __name__ == "__main__":
    main()
