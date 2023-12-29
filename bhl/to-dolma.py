"""Convert the raw ubuntu data to the dolma format."""

import argparse
import datetime
import glob
import logging
import json
import os

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma


logging.basicConfig(level=logging.INFO, format="to-dolma: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s")


BASE_URL = "https://www.biodiversitylibrary.org/page"
SOURCE_NAME = "biodiversity-heritage-library"

parser = argparse.ArgumentParser(description="Convert data to dolma.")
parser.add_argument(
    "--data", default=f"data/{SOURCE_NAME}/extracted_data", help="Path to the directory containing BHL data."
)
parser.add_argument(
    "--output_dir",
    default=f"data/{SOURCE_NAME}/v0",
    help="Where the dolma formatted data goes."
)
parser.add_argument(
    "--filename", default="bhl.jsonl.gz", help="The base filename for the BHL data"
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)


def format_dolma(content_file: str, source_name: str = SOURCE_NAME, base_url: str = BASE_URL):
    item_id, page_id, page_num = os.path.splitext(os.path.basename(content_file))[0].split("-") 
    with open(content_file) as f:
        try:
            text = f.read()
        except UnicodeDecodeError:
            # This should happen very rarely
            return None

    return {
        "id": f"{item_id}-{page_id}-{page_num}",
        "page_id": f"{page_id}",
        "item_id": f"{item_id}",
        "page_num": f"{page_num}",
        "text": text,
        "source": source_name,
        "added": datetime.datetime.utcnow().isoformat(),
        "metadata": {
            "license": str(PermissiveLicenses.PD),
            "url": f"{base_url}/{page_id}",
        },
    }


def main(args):
    # Use iterators so we don't have to load the whole dataset in memory.
    content_pages = filter(lambda x: x is not None, map(format_dolma, glob.iglob(os.path.join(args.data, "**", "*.txt"), recursive=True)))
    to_dolma(content_pages, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
