import argparse
from datetime import datetime

import datasets

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma


def format_dolma(row: dict) -> dict:
    return {
        "id": row["speechdate"].replace("-", ""),
        "text": row["text"],
        "created": row["speechdate"],
        "source": "canadian-hansard",
        "added": str(datetime.now().date()),
        "metadata": {
            "license": str(PermissiveLicenses.PD),
            "language": "en",
            "year": row["speechdate"].split("-")[0],
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Collect Canadian Hansard into Dolma format."
    )
    parser.add_argument(
        "--output_folder",
        default="data/hansard/ca",
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
