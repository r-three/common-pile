"""Populate the license cache from our data."""

import argparse
import glob
import itertools
import json
import shelve
import time

from ghapi.all import GhApi
from to_dolma import LicenseInfo, LicenseSnapshot, get_license_info, read_threads

from licensed_pile import logs

parser = argparse.ArgumentParser(description="Scrape Licenses.")
parser.add_argument(
    "--repo_list", help="Path to a file that has a list of repos (JSON).", required=True
)


def main():
    args = parser.parse_args()
    api = GhApi()
    logger = logs.configure_logging(level="INFO")
    with open(args.repo_list) as f:
        repos = json.load(f)
    wait = 60
    with shelve.open(f"{args.repo_list}.licenses") as license_cache:
        i = 0
        while True:
            while api.limit_rem == 0:
                logger.info(f"Waiting as API quota is low, {api.limit_rem}.")
                time.sleep(1)
            try:
                with logger(repo=repos[i], i=i):
                    _ = get_license_info(
                        repos[i], license_cache, api, fetch_license=True
                    )
                    i += 1
                    wait = max(60, wait // 8)
            except Exception as e:
                if "API rate limit exceeded" in e.msg:
                    wait = min(wait * 4, 60 * 60)
                    logger.info(f"API rate limit exceeded. Waiting {wait} seconds.")
                    time.sleep(wait)
                else:
                    logger.exception(f"Failed to process {repos[i]}, skipping")
                    i += 1


if __name__ == "__main__":
    main()
