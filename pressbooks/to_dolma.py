import argparse
import ast
import csv
import datetime
import glob
import json
import logging
import os
import re
import sys

import trafilatura
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

csv.field_size_limit(sys.maxsize)


logging.basicConfig(
    level=logging.INFO,
    format="to-dolma: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s",
)


SOURCE_NAME = "pressbooks"

parser = argparse.ArgumentParser(description="Convert data to dolma.")
parser.add_argument(
    "--filename",
    default="pressbooks.json.gz",
    help="The base filename for the BHL data",
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)


LICENSE_MAP = {
    "CC BY": PermissiveLicenses.CC_BY,
    "CC BY-SA": PermissiveLicenses.CC_BY_SA,
    "CC0": PermissiveLicenses.CC0,
    "Public Domain": PermissiveLicenses.PD,
}


def get_records():
    with open("pressbooks_content_no_duplicates.csv", "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            (
                title,
                author,
                subject,
                institution,
                language,
                license,
                last_updated,
                book_url,
                page_url,
                page_content,
            ) = row

            if license not in LICENSE_MAP:
                continue

            text = trafilatura.extract(page_content)
            if not text:
                continue

            yield {
                "id": page_url,
                "text": text,
                "source": SOURCE_NAME,
                "added": datetime.datetime.utcnow().isoformat(),
                "created": last_updated,
                "metadata": {
                    "license": str(LICENSE_MAP[license]),
                    "url": page_url,
                    "book_url": book_url,
                    "title": title,
                    "author": author,
                    "institution": institution,
                    "subject": subject,
                },
            }


def main(args):
    # Use iterators so we don't have to load the whole dataset in memory.
    to_dolma(get_records(), "./data/pressbooks", args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
