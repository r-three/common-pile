#!/usr/bin/env python3

import argparse
import dataclasses
import json
import shelve
from datetime import datetime
from functools import singledispatch

from tqdm import tqdm
from utils import LicenseInfo, LicenseSnapshot


@singledispatch
def encode(obj):
    return json.JSONEncoder().default(obj)


@encode.register(datetime)
def _(obj):
    return obj.isoformat()


@encode.register(LicenseInfo)
def _(obj):
    return dataclasses.asdict(obj)


parser = argparse.ArgumentParser(description="Convert a licensed cache to Json.")
parser.add_argument(
    "--license_cache", required=True, help="The shelf of licenses to dump."
)
parser.add_argument("--output", required=True, help="Where to output the licenses.")


def main():
    args = parser.parse_args()

    print(f"Dumping shelve licenses from {args.license_cache} to json at {args.output}")
    with shelve.open(args.license_cache) as license_cache:
        with open(args.output, "w") as wf:
            for repo, license_info in tqdm(license_cache.items()):
                wf.write(
                    json.dumps({"repo": repo, "license": license_info}, default=encode)
                    + "\n"
                )


if __name__ == "__main__":
    main()
