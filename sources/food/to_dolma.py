"""Convert the downloaded pages into the dolma format."""


import argparse
import datetime
import functools
import json
import os

from common_pile import licenses, logs
from common_pile.write import to_dolma

SOURCE_NAME = "foodista"
LICENSE_MAP = {"CC-BY-3": licenses.PermissiveLicenses.CC_BY_3}

parser = argparse.ArgumentParser(
    description="Convert downloaded pages to the dolma format."
)
parser.add_argument(
    "--index_path",
    default="data/pages/page_index.jsonl",
    help="The list of files we have downloaded from the site.",
)
parser.add_argument("--input_dir", help="Where the downloaded pages live on disk.")
parser.add_argument(
    "--output_dir",
    default=f"data/{SOURCE_NAME}/raw/documents",
    help="Where the resulting dolma dataset will live.",
)
parser.add_argument(
    "--filename",
    default=f"{SOURCE_NAME}.jsonl.gz",
    help="The name of the shards in the dolma dataset.",
)
parser.add_argument(
    "--shard_size", default=1, type=int, help="The size, in GB, each shard should be."
)
parser.add_argument(
    "--license",
    choices=["CC-BY-3"],
    default="CC-BY-3",
    type=lambda l: LICENSE_MAP[l],
    help="The license the site is distributed under.",
)


def format_page(
    page_info,
    input_dir: str,
    today: datetime.datetime,
    license: licenses.PermissiveLicenses,
    source_name: str = SOURCE_NAME,
):
    """Create a dolma record for each page."""
    logger = logs.get_logger("food")
    page_path = os.path.join(input_dir, page_info["filename"])
    logger.info(f"Reading {page_info['url']} from {page_info['filename']}")
    if os.path.exists(page_path):
        with open(page_path) as f:
            html = f.read()

        return {
            "id": page_info["idx"],
            "text": html,
            "source": source_name,
            "added": today.isoformat(),
            "created": None,  # This will be filled in later.
            "metadata": {
                "license": str(license),
                "url": page_info["url"],
                "authors": None,  # This will be filled in later.
            },
        }
    # We handled test runs by only having a few eaxmples downloaded, thus this
    # log message is common when doing test runs.
    else:
        logger.error(f"Article {page_path} exists in the index but is not downloaded.")


def main(args):
    args.input_dir = (
        args.input_dir
        if args.input_dir is not None
        else os.path.dirname(args.index_path)
    )
    with open(args.index_path) as f:
        page_index = [json.loads(l) for l in f]

    os.makedirs(args.output_dir, exist_ok=True)
    today = datetime.datetime.utcnow()

    # The main function of this code is read from disk, write to disk, there is
    # little computation done, thus parallelism will not help much due to read/write
    # contention. Therefore we skip it.
    pages = map(
        functools.partial(
            format_page,
            input_dir=args.input_dir,
            today=today,
            license=args.license,
            source_name=SOURCE_NAME,
        ),
        page_index,
    )
    pages = filter(lambda p: p is not None, pages)
    to_dolma(pages, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("food")
    main(args)
