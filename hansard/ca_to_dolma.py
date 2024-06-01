import argparse
from datetime import datetime

import datasets
import regex

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

WHITESPACE = regex.compile(r"\w+|[^\w\s]+")
COUNT = 0


def format_dolma(row: dict) -> dict:
    global COUNT
    COUNT += len(WHITESPACE.split(row["text"]))
    return {
        "id": row.get("speechdate").strftime("%Y-%m-%d").replace("-", ""),
        "text": row["text"],
        "created": row["speechdate"].strftime("%Y-%m-%d"),
        "source": "ca-hansard",
        "added": str(datetime.now().date()),
        "metadata": {
            "license": str(PermissiveLicenses.PD),
            "language": "en",
            "year": str(row["speechdate"].year),
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect Canadian Hansard into Dolma format."
    )
    parser.add_argument(
        "--output_folder",
        default="hansard/ca_hansard",
        help="Output format for parquet files",
    )
    parser.add_argument(
        "--file-name",
        default="cahansard.jsonl.gz",
        help="The base filename for our books.",
    )
    parser.add_argument(
        "--shard-size", type=int, default=1, help="Size, in GB, for each shard."
    )
    parser.add_argument(
        "--count",
        action="store_true",
        help="Count the number of words in the dataset.",
    )
    args = parser.parse_args()
    # Load the dataset
    dataset = datasets.load_dataset(
        "baber/canadian_hansard",
        split="train",
    )
    ds = dataset.map(format_dolma, remove_columns=dataset.column_names)
    to_dolma(
        ds, path=args.output_folder, filename=args.file_name, shard_size=args.shard_size
    )
    if args.count:
        print(COUNT)
