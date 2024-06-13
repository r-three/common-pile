"""Parse pages from html to plain text and format with dolma."""

import argparse
import functools
import json
import multiprocessing as mp
import os
from datetime import datetime

import utils
from charset_normalizer import from_bytes

from licensed_pile import licenses, logs
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Parse pages downloaded from a News Sites")
parser.add_argument(
    "--index_path",
    required=True,
    help="Path to the index file of the pages in the site.",
)
parser.add_argument(
    "--input_dir",
    help="Where the downloaded pages live. Defaults to the same dir as --index_path.",
)
parser.add_argument(
    "--output_dir",
    required=True,
    help="Path to output directory for processed data.",
)
parser.add_argument("--source_name", required=True, help="The name of the datasource.")
parser.add_argument("--filename", help="The base filename for our data.")
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)
parser.add_argument("--license", type=str, default="CC-BY", help="Type of license")
parser.add_argument(
    "--tag", type=str, default="div", help="Tag for the article or content"
)
parser.add_argument("--attrs", type=json.loads, default=None, help="dict of attributes")
parser.add_argument(
    "--num_workers",
    default=mp.cpu_count(),
    help="Number of workers",
)


LICENSE_MAP = {
    "CC-BY": licenses.PermissiveLicenses.CC_BY,
    "CC-BY-SA": licenses.PermissiveLicenses.CC_BY_SA,
    "Public Domain": licenses.PermissiveLicenses.PD,
}


def parse_page(
    page_index,
    input_dir: str,
    today: datetime,
    license_type: licenses.PermissiveLicenses,
    source_name: str,
    tag: str = "div",
    attrs=None,
):
    idx = page_index["idx"]
    url = page_index["url"]
    filename = page_index["filename"]

    logger = logs.get_logger("news")
    html_path = os.path.join(input_dir, filename)

    if not utils.filter_url(url):
        return

    logger.info(f"Parsing article in {html_path}")
    if os.path.exists(html_path):
        with open(html_path, "rb") as f:
            html = str(from_bytes(f.read()).best())
        # TODO: Clean up date and author field.
        text, date, author = utils.parse_page(html, tag=tag, attrs=attrs)

        return {
            "id": idx,
            "text": text,
            "source": source_name,
            "added": today.isoformat(),
            "created": date,  # date.isoformat(),
            "metadata": {
                "license": str(license_type),
                "url": url,
                "author": author,
            },
        }
    else:
        logger.warning(f"Article {url} exists in the index but is not downloaded.")


def main(args):
    logger = logs.get_logger("news")
    args.input_dir = (
        args.input_dir
        if args.input_dir is not None
        else os.path.dirname(args.index_path)
    )
    with open(args.index_path) as f:
        page_index = [json.loads(line) for l in f if (line := l.strip())]

    if not page_index:
        logger.error(f"{args.index_path} is empty.")
        raise ValueError(f"{args.index_path} is empty.")

    os.makedirs(args.output_dir, exist_ok=True)
    today = datetime.utcnow()

    args.filename = (
        args.filename if args.filename is not None else f"{args.source_name}.jsonl.gz"
    )

    with mp.Pool(args.num_workers) as p:
        page_data = p.imap(
            functools.partial(
                parse_page,
                input_dir=args.input_dir,
                today=today,
                license_type=LICENSE_MAP[args.license],
                tag=args.tag,
                source_name=f"news-{args.source_name}",
                attrs=args.attrs,
            ),
            page_index,
        )
        page_data = filter(lambda p: p is not None, page_data)

        to_dolma(page_data, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("news")
    main(args)
