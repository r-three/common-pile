"""Populate the license cache from our data."""

import argparse
import glob
import itertools
import json
import shelve
import time
from datetime import datetime, timezone

from dateutil.parser import parse
from ghapi.all import GhApi
from to_dolma import (
    LicenseInfo,
    LicenseSnapshot,
    get_license_info,
    read_threads,
    check_github_graphql_rate_limit,
    batched_get_license_info,
)

from licensed_pile import logs

parser = argparse.ArgumentParser(description="Scrape Licenses.")
parser.add_argument(
    "--repo_list", help="Path to a file that has a list of repos (JSON).", required=True
)
parser.add_argument(
    "--batch_size", help="Batch requests using GraphQL API.", required=False, default=200, type=int
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
        # Initial rate limit check only needed for the first iteration
        rate_limit = check_github_graphql_rate_limit() if args.batch_size > 1 else None
        logger.info(f"Rate limit: {rate_limit}")
        if args.batch_size > 1:
            while i < len(repos):
                # Check if we need to wait for rate limit reset
                if rate_limit and rate_limit["remaining"] < args.batch_size:
                    logger.info(
                        f"Rate limit is low, {rate_limit['remaining']}. Waiting until {rate_limit['resetAt']}."
                    )
                    reset_time = parse(rate_limit["resetAt"])
                    sleep_seconds = (
                        reset_time - datetime.now(timezone.utc)
                    ).total_seconds()
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)

                batch_size = min(args.batch_size, len(repos) - i)

                if batch_size > 0:
                    *_, rate_limit = batched_get_license_info(
                        repos[i : i + batch_size], license_cache, rate_limit=rate_limit
                    )
                    i += batch_size
                else:
                    break
        else:
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
                    error = str(e)
                    if "API rate limit exceeded" in error:
                        wait = min(wait * 4, 60 * 60)
                        logger.info(f"API rate limit exceeded. Waiting {wait} seconds.")
                        time.sleep(wait)
                    else:
                        logger.exception(f"Failed to process {repos[i]}, skipping")
                        i += 1


if __name__ == "__main__":
    main()
