"""Merge pickle licenses into the main cache."""


import argparse
import glob
import pickle
import shelve

from tqdm import tqdm
from utils import LicenseInfo, LicenseSnapshot

parser = argparse.ArgumentParser(
    description="Merge the pickle dumps from the distributed scrape."
)
parser.add_argument(
    "--license_cache", required=True, help="The license file to add the other to."
)
parser.add_argument(
    "--license_shards", required=True, help="A pattern of pickle files to load."
)


def main():
    args = parser.parse_args()

    with shelve.open(args.license_cache) as license_cache:
        for file_path in glob.glob(args.license_shards):
            print(f"Loading licenses from {file_path}")
            with open(file_path, "rb") as f:
                licenses = pickle.load(f)
                for repo, license_info in tqdm(licenses.items()):
                    if repo in license_cache:
                        print(f"Repo {repo} already in cache, skipping.")
                    for l in license_info.licenses:
                        l.license_source = "github-api"
                    license_cache[repo] = license_info


if __name__ == "__main__":
    main()
