"""..."""

import glob
import json
import operator as op
import shelve

import smart_open
from tqdm import tqdm


def read_repos(pattern):
    for file_name in glob.iglob(pattern):
        with smart_open.open(file_name, compression=".gz") as f:
            yield from [json.loads(l) for l in f if l]


def main():
    repos = list(
        read_repos("data/bigquery/thread-distribution/thread-distribution.jsonl-gz-*")
    )
    repos = sorted(repos, key=op.itemgetter("thread_count"), reverse=True)
    to_be_scraped = []
    with shelve.open("data/license_cache.ck") as license_cache:
        for repo in tqdm(repos):
            if "license" in repo:
                continue
            if repo["repo_name"] not in license_cache:
                to_be_scraped.append(repo["repo_name"])
    with open("data/repos-to-scrape.json", "w") as wf:
        json.dump(to_be_scraped, wf)


if __name__ == "__main__":
    main()
