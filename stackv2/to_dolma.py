import argparse
import os
from datetime import datetime

from datasets import load_dataset

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.logs import configure_logging
from licensed_pile.write import to_dolma

SOURCE_NAME = "The Stack v2"

logger = configure_logging("court-listener-opinion")

LICENSE_MAP = {
    "Apache-2.0": PermissiveLicenses.APACHE_2,
    "BSD-2-Clause": PermissiveLicenses.BSD,
    "MIT License": PermissiveLicenses.MIT,
}


def main(args):
    ds = load_dataset(args.data_path, data_dir=args.data_dir, split="train")
    ds.filter(lambda x: x["license"] in LICENSE_MAP.keys())
    ds = ds.map(
        lambda x: {
            "id": x["id"],
            "text": x["content"],
            "source": SOURCE_NAME,
            "added": datetime.utcnow().isoformat(),
            "created": "2024-02-29",
            "metadata": {
                "license": str(LICENSE_MAP[x["license"]]),
                "url": "https://github.com/" + x["repo"] + "/tree/" + x["revision_id"],
            },
        }
    )

    output_file_base_name = os.path.join(args.output_dir, "starcoder2data.jsonl.gz")
    to_dolma(ds, args.output_dir, output_file_base_name, args.shard_size)
    logger.info(f"Saved {args.data_path} as dolma sharded files at {args.output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert csv data to dolma.")
    parser.add_argument(
        "--output_dir",
        default=f"data/stackv2",
        help="Where the dolma formatted data goes.",
    )
    parser.add_argument(
        "--data_path",
        default="bigcode/starcoder2data",
        help="HF dataset path to the data",
    )
    parser.add_argument(
        "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
    )
    parser.add_argument(
        "--data_dir", type=str, default=None, help="Optionally select a language"
    )
    args = parser.parse_args()
    main(args)
