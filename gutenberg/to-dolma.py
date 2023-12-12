"""Convert the index+data into the dolma sharded jsonl.gz format."""

import argparse
import functools
import json
import operator as op
import os
from datetime import datetime

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Collect PG Books into Dolma format.")
parser.add_argument(
    "--index", default="data/books.json", help="Path to our index file."
)
parser.add_argument(
    "--book_dir", default="data/raw_books", help="Path to our directory of raw books."
)
parser.add_argument(
    "--output_dir",
    default="data/project-gutenberg/raw/documents/",
    help="Where the dolma formatted data goes.",
)
parser.add_argument(
    "--filename", default="pg.jsonl.gz", help="The base filename for our books."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)

SOURCE_NAME = "project gutenberg"


def format_dolma(example, book_dir: str, source_name: str = SOURCE_NAME):
    path = os.path.join(book_dir, f"{example['id']}.txt")
    try:
        with open(path) as f:
            text = f.read()
    # Some data has utf-8 in the metadata but seems to be latin-1 encoded?
    # Once we read and write it, it will all be utf-8 tho.
    # TODO: If we run into this kinda stuff a lot then look into a library like
    # chardet.
    except UnicodeDecodeError as e:
        with open(path, encoding="latin-1") as f:
            text = f.read()
    return {
        "id": example["id"],
        "text": text,
        "source": source_name,
        "added": datetime.utcnow().isoformat(),
        "metadata": {
            "license": str(PermissiveLicenses.PD),
            "language": example.get("lang"),
            "url": example.get("file"),
            "title": example.get("title"),
        },
    }


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    with open(args.index) as f:
        index = json.load(f)

    examples = map(functools.partial(format_dolma, book_dir=args.book_dir), index)
    examples = sorted(examples, key=op.itemgetter("id"))

    to_dolma(examples, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
