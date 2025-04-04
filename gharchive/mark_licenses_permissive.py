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
    "--output", required=True, help="Where to save the new license file."
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
            licenses.add(license["id"].lower())
    return licenses


def main():
    args = parser.parse_args()

    with open(args.blue_oak) as f:
        blue_oak = json.load(f)

    blue_oak = parse_blue_oak(blue_oak)
    # Not in list but OSS complient.
    blue_oak.add("cdla_p")
    blue_oak.add("CC-BY-4.0".lower())

    with open(args.license_cache) as f, open(args.output, "w") as wf:
        for line in tqdm(f):
            if line:
                data = json.loads(line)
                license = data["license"]
                licenses = license["licenses"]
                # Filter our the Nones.
                licenses = [l for l in licenses if l["license"] is not None]
                license["licenses"] = licenses
                # Pass through stack settings
                if any(l["license_source"] == "stackv2" for l in licenses):
                    continue
                if all(l["license"].lower() in blue_oak for l in licenses):
                    license["license_type"] = "permissive"
                else:
                    license["license_type"] = "restrictive"
                wf.write(json.dumps({"repo": data["repo"], "license": license}) + "\n")

#     with shelve.open(args.license_cache, writeback=True) as license_cache:
#         for repo, license_info in tqdm(license_cache.items()):
#             if license_info.license_type != "":
#                 continue
#             if not license_info.licenses:
#                 raise ValueError(f"Repo {repo} has no license information.")
#             if all(l.license.lower() in blue_oak for l in license_info.licenses):
#                 license_info.license_type = "permissive"
#             else:
#                 license_info.license_type = "restrictive"
#             license_cache.sync()


if __name__ == "__main__":
    main()
