#!/usr/bin/env python3

import argparse
import shelve

import datasets
from tqdm import tqdm

parser = argparse.ArgumentParser(
    description="Collect License Information from the StackV2."
)
parser.add_argument("--dataset", default="nkandpa2/common-pile-filtered", help="")
parser.add_argument("--output", default="data/stack_licenses", help="")


def main():
    args = parser.parse_args()
    with shelve.open(args.output) as license_cache:
        ds = datasets.load_dataset(args.dataset, "stackv2", streaming=True)
        for example in tqdm(ds["train"]):
            if (repo := example["metadata"]["repo_name"]) not in license_cache:
                license_cache[repo] = {
                    "detected_licenses": example["metadata"]["detected_licenses"],
                    "gha_license_id": example["metadata"]["gha_license_id"],
                    "license": example["metadata"]["license"],
                    "license_type": example["metadata"]["license_type"],
                }


if __name__ == "__main__":
    main()
