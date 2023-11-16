"""Download PG books."""

import argparse
import json
import os
import time
import urllib.request
import tqdm

parser = argparse.ArgumentParser(description="Download PG books.")
parser.add_argument("--index", default="data/books.json")
parser.add_argument("--output_dir", default="data/raw_books")
parser.add_argument("--overwrite", action="store_true")


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    with open(args.index) as f:
        index = json.load(f)

    for i, entry in enumerate(tqdm.tqdm(index)):
        output_file = os.path.join(args.output_dir, f"{entry['id']}.txt")

        if os.path.exists(output_file) and not args.overwrite:
            continue

        data = urllib.request.urlopen(entry["file"])

        with open(output_file, "wb") as wf:
            wf.write(data.read())

        time.sleep(2)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
