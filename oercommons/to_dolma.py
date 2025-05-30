import argparse
import datetime
import glob
import json
import logging
import os
import csv
import sys

import trafilatura
from bs4 import BeautifulSoup

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma


csv.field_size_limit(sys.maxsize)


logging.basicConfig(
    level=logging.INFO,
    format="to-dolma: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s",
)


SOURCE_NAME = "oercommons"
LICENSE_MAP = {
        "Public Domain": PermissiveLicenses.PD,
        "CC BY": PermissiveLicenses.CC_BY,
        "CC BY-SA": PermissiveLicenses.CC_BY_SA
}

parser = argparse.ArgumentParser()
parser.add_argument(
    "--filename", default="oercommons.json.gz", help="The base filename for the BHL data"
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)


def get_records():
    with open("oercommons_content.csv", "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            title, search_result_link, content_link, content_html, metadata_html = row
            metadata = {}
            metadata_soup = BeautifulSoup(metadata_html, "html.parser")
            for dt, dd in zip(metadata_soup.find_all("dt"), metadata_soup.find_all("dd")):
                key = dt.get_text(strip=True).rstrip(":")
                value = dd.get_text(strip=True)
                if key in ["Subject", "Material Type", "Author", "Date Added"]:
                    metadata[key] = value

            license = LICENSE_MAP[metadata_soup.find_all("span")[0].get_text().strip()]
            text = trafilatura.extract(content_html)
            if text:
                text_lines = text.split("\n")
                while len(text_lines) > 0 and text_lines[0].startswith("- "):
                    text_lines.pop(0)
                text = "\n".join(text_lines)

                yield {
                        "id": content_link,
                        "text": text,
                        "source": SOURCE_NAME,
                        "added": datetime.datetime.utcnow().isoformat(),
                        "created": metadata.get("Date Added"),
                        "metadata": {
                            "license": str(license),
                            "url": content_link,
                            "title": title,
                            "author": metadata.get("Author")
                        }
                    }


def main(args):
    # Use iterators so we don't have to load the whole dataset in memory.
    to_dolma(get_records(), "./data/oercommons", args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
