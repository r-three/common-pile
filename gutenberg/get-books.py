"""Download PG books.

Note that we don't download in parallel as PG requests that people wait between
bulk downloads.
"""

import argparse
import json
import operator as op
import os
import time
import urllib.request

import tqdm

# This books is missing on PG, we will get it later from pg19, so skip for now.
SKIP = (51155,)

parser = argparse.ArgumentParser(description="Download PG books.")
parser.add_argument(
    "--index", default="data/books.json", help="Path to our index file."
)
parser.add_argument(
    "--output_dir",
    default="data/raw_books",
    help="Path to output directory where raw books are downloaded.",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously downloaded copies?",
)
parser.add_argument(
    "--wait", default=2, type=int, help="Time to wait between requests (seconds)."
)
parser.add_argument(
    "--skip", default=SKIP, nargs="+", help="Book ids to skip downloading."
)


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)
    skip = set(args.skip)

    with open(args.index) as f:
        index = json.load(f)

    index = sorted(index, key=op.itemgetter("id"))

    for i, entry in enumerate(tqdm.tqdm(index)):
        if entry["id"] in skip:
            continue

        output_file = os.path.join(args.output_dir, f"{entry['id']}.txt")

        if os.path.exists(output_file) and not args.overwrite:
            continue

        data = urllib.request.urlopen(entry["file"])

        with open(output_file, "wb") as wf:
            wf.write(data.read())

        time.sleep(args.wait)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
