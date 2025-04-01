#!/usr/bin/env python3

import argparse
import json
import shelve

from tqdm import tqdm

parser = argparse.ArgumentParser(
    description="Make collected licenses as permissive or not."
)
parser.add_argument(
    "--license_cache",
    default="data/license_cache",
    help="The license cache as a shelf.",
)
parser.add_argument(
    "--blue_oak",
    default="data/blue_oak.json",
    help="The Blue Oak Council license information as JSON.",
)


def parse_blue_oak(blue_oak: dict) -> set:
    licenses = set()
    for rank in blue_oak["ratings"]:
        for license in rank["licenses"]:
            licenses.add(license["name"].lower())
    return licenses


def main():
    args = parser.parse_args()

    with open(args.blue_oak) as f:
        blue_oak = json.load(f)

    blue_oak = parse_blue_oak(blue_oak)
    # Not in list but OSS complient.
    blue_oak.add("cdla_p")

    with shelve.open(args.license_cache, writeback=True) as license_cache:
        for repo, license_info in tqdm(license_cache.items()):
            if license_info.license_type != "":
                continue
            if not license_info.licenses:
                raise ValueError(f"Repo {repo} has no license information.")
            if all(l.license.lower() in blue_oak for l in license_info.licenses):
                license_info.license_type = "permissive"
            else:
                license_info.license_type = "restrictive"
            license_cache.sync()


if __name__ == "__main__":
    main()
