"""Create a list of repos from the big query export."""

import argparse
import glob
import itertools
import json

from to_dolma import read_threads
from tqdm import tqdm

parser = argparse.ArgumentParser(description="Collect repos from the bigquery export.")
parser.add_argument(
    "--bq", required=True, help="Pattern that picks up to the bigquery exported files."
)
parser.add_argument(
    "--output", default="data/repos.json", help="Where to save the found repos."
)


def main():
    args = parser.parse_args()
    repos = set()
    files = glob.iglob(args.bq)
    for thread in tqdm(itertools.chain(*map(read_threads, files))):
        repos.add(thread["repo_name"])
    print(f"{len(repos)} repos found.")
    with open(args.output, "w") as wf:
        json.dump(sorted(repos), wf)


if __name__ == "__main__":
    main()
