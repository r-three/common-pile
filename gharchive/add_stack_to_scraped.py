"""Combine the stackv2 license info with the scrapped info from github."""

import argparse
import itertools
import shelve
from datetime import datetime

from tqdm import tqdm
from utils import LicenseInfo, LicenseSnapshot

from licensed_pile import logs

parser = argparse.ArgumentParser(
    description="Combine the raw license info from the stack v2 with github scrapped data."
)
parser.add_argument(
    "--github",
    default="data/license_cache",
    help="Licenses from Github scraping as shelve.",
)
parser.add_argument(
    "--stack", default="data/stack_licenses", help="Licenses from the Stack v2 shelve."
)


def process_licenses(license_info):
    alpha = datetime(1900, 1, 1)
    omega = datetime(9999, 1, 1)
    licenses = []
    for license in set(
        itertools.chain(
            license_info["detected_licenses"], [license_info["gha_license_id"]]
        )
    ):
        licenses.append(
            LicenseSnapshot(license, start=alpha, end=omega, license_source="stackv2")
        )
    return LicenseInfo(licenses, license_info["license_type"])


def main():
    args = parser.parse_args()
    logger = logs.configure_logging()
    with shelve.open(args.github) as github, shelve.open(args.stack) as stack:
        og_scrape_size = len(github)
        updated_by_stack = 0
        for repo, license_info in tqdm(stack.items()):
            license_info = process_licenses(license_info)
            if repo in github:
                github_license = github[repo]
                if github_license == license_info:
                    continue
                logger.warning(
                    f"Repo {repo} has already be scrapped, updating license info from {github[repo]} to {license_info}"
                )
                updated_by_stack += 1
            github[repo] = license_info
        new_scrape_size = len(github)
    print(f"Added {new_scrape_size - og_scrape_size:,} new repos to the license cache.")
    print(f"{updated_by_stack:,} repos had their license info update by the stack.")


if __name__ == "__main__":
    main()
