#!/usr/bin/env python3

import argparse
import json

parser = argparse.ArgumentParser()
parser.add_argument("--repos", default="data/repos-to-scrape.json")
parser.add_argument("--shards", type=int, default=30)


def main():
    args = parser.parse_args()
    with open(args.repos) as f:
        repos = json.load(f)

    for shard in range(args.shards):
        with open(f"{args.repos}-{shard:>05}", "w") as wf:
            shard_repos = [r for i, r in enumerate(repos) if i % args.shards == shard]
            print(f"{len(shard_repos)} repos in shard {shard}")
            json.dump(shard_repos, wf)


if __name__ == "__main__":
    main()
