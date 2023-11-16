"""Preprocess books."""

import argparse
import re
import glob
import os
import tqdm
import gutenbergpy.textget

parser = argparse.ArgumentParser(description="preprocess downloaded books.")
parser.add_argument("--raw", default="data/raw_books")
parser.add_argument("--output", default="data/books")
parser.add_argument("--overwrite", action="store_true")


def preprocess(text):
    # This library leaves license text on several books such as 71224.txt
    return gutenbergpy.textget.strip_headers(text.encode("utf-8")).decode("utf-8")


def main(args):
    i = 0
    for book in tqdm.tqdm(glob.iglob(os.path.join(args.raw, "*.txt"))):
        # Jumping to specific books to test it out, remove in final version.
        if i != 6:
            i += 1
            continue
        with open(book) as f:
            t = preprocess(f.read())
            print(t)
            print(book)
        break


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
