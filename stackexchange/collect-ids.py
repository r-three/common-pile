"""Collect all ids from dolma files."""

import argparse
import glob
import json
import smart_open
from tqdm import tqdm
from licensed_pile import utils


parser = argparse.ArgumentParser(description="Collect all ids from dolma files.")
parser.add_argument(
    "--input",
    required=True,
    help="The input dir.")
parser.add_argument(
    "--output",
    default="ids.json",
    help="Where to save the resulting file."
)
parser.add_argument(
    "--filename",
    default="*.jsonl.gz",
    help="The default file name glob pattern."
)


def main():
    args = parser.parse_args()
    
    ids = set()
    for file_name in tqdm(glob.glob(utils.dolma_input(args.input, args.filename))):
        with smart_open.open(file_name) as f:
            for line in f:
                if line:
                    data = json.loads(line)
                    ids.add(data["id"])
    with open(args.output, "w") as wf:
        json.dump(list(ids), wf)


if __name__ == "__main__":
    main()
