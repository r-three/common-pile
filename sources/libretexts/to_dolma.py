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
from bs4 import BeautifulSoup

from common_pile.licenses import PermissiveLicenses
from common_pile.write import to_dolma

csv.field_size_limit(sys.maxsize)


logging.basicConfig(
    level=logging.INFO,
    format="to-dolma: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s",
)


SOURCE_NAME = "libretexts"

parser = argparse.ArgumentParser(description="Convert data to dolma.")
parser.add_argument(
    "--filename", default="libretext.json.gz", help="The base filename for the BHL data"
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)


def get_license(name, version=None):
    if name == "ccby":
        if version == "30":
            return PermissiveLicenses.CC_BY_3
        return PermissiveLicenses.CC_BY
    elif name == "ccbysa":
        if version == "30":
            return PermissiveLicenses.CC_BY_SA_3
        elif version == "25":
            return PermissiveLicenses.CC_BY_SA_2_5
        return PermissiveLicenses.CC_BY_SA
    elif name == "gnufdl":
        return PermissiveLicenses.GFDL
    elif name == "publicdomain":
        return PermissiveLicenses.PD
    return None


def get_records():
    with open("libretext_content.csv", "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in reader:
            book_url, section_name, section_url, content = row
            soup = BeautifulSoup(content, "html.parser")
            page_tags_div = soup.find("div", {"id": "pageTagsHolder"})
            if not page_tags_div:
                continue
            raw_metadata = page_tags_div.text.strip()
            metadata_list = ast.literal_eval(raw_metadata)
            metadata_dict = {
                item.split(":")[0]: item.split(":")[1]
                for item in metadata_list
                if ":" in item
            }

            license_short = metadata_dict.get("license")
            license_version = metadata_dict.get("licenseversion")
            license = get_license(license_short, license_version)
            if not license:
                continue

            author_tag = soup.find("li", class_="mt-author-information")
            author = author_tag.text if author_tag else None

            title_tag = soup.find("meta", property="og:title")
            title = title_tag["content"] if title_tag else None

            site_tag = soup.find("meta", property="og:site_name")
            site = site_tag["content"] if site_tag else None

            published_time_tag = soup.find("meta", property="article:published_time")
            published_time = (
                published_time_tag["content"] if published_time_tag else None
            )

            for div in soup.find_all("div", class_="Headertext"):
                div.clear()

            text = trafilatura.extract(soup.prettify())
            if text is None:
                continue
            text = re.sub("- Page ID\s*", "", text, count=1)
            text = re.sub("- \d+\s*", "", text, count=1)

            yield {
                "id": section_url,
                "text": text,
                "source": SOURCE_NAME,
                "added": datetime.datetime.utcnow().isoformat(),
                "created": published_time,
                "metadata": {
                    "license": str(license),
                    "url": section_url,
                    "book_url": book_url,
                    "title": title,
                    "author": author,
                },
            }


def main(args):
    # Use iterators so we don't have to load the whole dataset in memory.
    to_dolma(get_records(), "./data/libretexts", args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
