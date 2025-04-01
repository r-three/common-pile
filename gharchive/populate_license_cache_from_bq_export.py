"""Pre-populate the repo license cache with infomation from a big query export."""

import argparse
import glob
import json
import shelve
from datetime import datetime
from typing import Iterator

import smart_open
from tqdm import tqdm
from utils import LicenseInfo, LicenseSnapshot

parser = argparse.ArgumentParser(description="Prepopulate repo license cache.")
parser.add_argument(
    "--bq",
    required=True,
    help="Pattern that picks up the bigquery exported license information.",
)
parser.add_argument(
    "--license_cache",
    default="data/license_cache",
    help="Where to cache the license data.",
)


def read_repo_data(pattern: str) -> Iterator[dict]:
    for file_name in glob.iglob(pattern):
        with smart_open.open(file_name, compression=".gz") as f:
            for line in f:
                if line:
                    yield json.loads(line)


def main():
    args = parser.parse_args()
    alpha = datetime(1900, 1, 1)
    omega = datetime(9999, 1, 1)
    source = "bigquery"
    with shelve.open(args.license_cache) as license_cache:
        for repo_data in tqdm(read_repo_data(args.bq)):
            repo = repo_data["repo_name"]
            if repo in license_cache:
                print(f"Repo {repo} already in cache")
                continue
            license_info = LicenseInfo(
                licenses=[
                    LicenseSnapshot(
                        license=repo_data["license"],
                        start=alpha,
                        end=omega,
                        license_source=source,
                    )
                ]
            )
            license_cache[repo] = license_info


if __name__ == "__main__":
    main()
