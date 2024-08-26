"""Convert Arxiv Dumps into the dolma format."""

import argparse
import datetime

import jsonlines

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.logs import configure_logging
from licensed_pile.write import to_dolma
from utils import LICENSE_MAP, parse_date


SOURCE_NAME = "arxiv-abstracts"


parser = argparse.ArgumentParser(description="Convert abstracts from Arxiv metadata dump to the dolma format.")
parser.add_argument("--metadata-file", default=f"data/{SOURCE_NAME}/raw/arxiv-metadata-oai-snapshot.json", help="Path to ArXiv metadata file")
parser.add_argument("--output-dir", default=f"data/{SOURCE_NAME}/v0", help="Where the dolma formatted data goes")
parser.add_argument("--filename", default="arxiv-abstracts.jsonl.gz", help="The base filename for the ArXiv Abstracts data")
parser.add_argument("--shard-size", type=int, default=1, help="Size, in GB, for each shard.")


def generate_records(metadata_file):
    with jsonlines.open(metadata_file, "r") as reader:
        for record in reader:
            yield {
                "id": record["id"],
                "text": record["abstract"],
                "source": SOURCE_NAME,
                "created": parse_date(record["versions"][0]["created"]),
                "added": datetime.datetime.utcnow().isoformat(),
                "metadata": {
                    "license": str(PermissiveLicenses.CC0),
                    "full_text_license": str(LICENSE_MAP.get(record["license"])),
                    "authors": record["authors"],
                    "submitter": record["submitter"],
                    "url": f"https://arxiv.org/abs/{record['id']}"
                }
            }


def main(args):
    to_dolma(generate_records(args.metadata_file), args.output_dir, args.filename, args.shard_size) 


if __name__ == "__main__":
    args = parser.parse_args()
    configure_logging("arxiv-abstracts")
    main(args)
