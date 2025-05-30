"""Collect all ids from dolma files."""

import argparse
import glob
import json
import os

import smart_open
from tqdm import tqdm

from common_pile import utils
from common_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Collect all ids from dolma files.")
parser.add_argument("--input", required=True, help="The input dir.")
parser.add_argument("--old", required=True, help="The old dolma data.")
parser.add_argument(
    "--ids", default="ids.json", help="Where to save the resulting file."
)
parser.add_argument(
    "--filename", default="*.jsonl.gz", help="The default file name glob pattern."
)


def find_missing_examples(input_dir, filename, ids):
    for file_name in tqdm(glob.glob(utils.dolma_input(input_dir, filename))):
        with smart_open.open(file_name) as f:
            for line in f:
                if line:
                    data = json.loads(line)
                    if data["id"] not in ids:
                        yield data


def next_shard(input_dir, filename):
    files = glob.glob(utils.dolma_input(input_dir, filename))
    shard_idx = [int(os.path.basename(f)[:5]) for f in files]
    return max(shard_idx) + 1


def main():
    args = parser.parse_args()

    with open(args.ids) as f:
        ids = set(json.load(f))

    shard_idx = next_shard(args.input, args.filename)

    missing = find_missing_examples(args.old, args.filename, ids)
    pattern = utils.dolma_input(args.input, args.filename)
    output_dir = os.path.dirname(pattern)
    filename = os.path.basename(next(glob.iglob(pattern)))[6:]
    to_dolma(missing, output_dir, filename, shard_idx=shard_idx)


if __name__ == "__main__":
    main()
