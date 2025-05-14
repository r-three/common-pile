"""Download all the files from a site."""

import argparse
import functools
import json
import multiprocessing.dummy as mp
import os
import random
import time

import utils

from common_pile import logs, scrape

parser = argparse.ArgumentParser(description="Download pages from a news site.")
parser.add_argument(
    "--index_path",
    required=True,
    help="File that list of all pages",
)
parser.add_argument(
    "--output_dir",
    help="Path to output directory where raw pages are downloaded.",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously downloaded copies?",
)
parser.add_argument(
    "--num_workers",
    type=int,
    default=32,
    help="Number of workers",
)
parser.add_argument(
    "--test_run",
    type=int,
    help="Set number of pages",
)
parser.add_argument(
    "--wait",
    type=int,
    default=1,
    help="Time to wait between requests.",
)
parser.add_argument(
    "--dry_run", action="store_true", help="Don't actually download anything."
)


def get_pages(
    page_index, output_dir, overwrite: bool = True, wait: int = 0, dry_run: bool = False
):
    idx = page_index["idx"]
    url = page_index["url"]
    filename = page_index["filename"]

    page_file_path = os.path.join(output_dir, filename)
    logger = logs.get_logger("news")

    if not utils.filter_url(url):
        return

    if not overwrite and os.path.exists(page_file_path):
        logger.info(f"{page_file_path} already exists, not downloading.")
        return

    if dry_run:
        logger.info(f"Not downloading {url} as --dry_run was set.")
        return
    try:
        logger.info(f"Downloading {url}")
        page = scrape.get_page(url)
        with open(page_file_path, "wb") as fp:
            fp.write(page.content)
    except Exception as err:
        logger.error(f"Failed to fetch {url}")
    if wait:
        time.sleep(wait)


def main(args):
    args.output_dir = (
        args.output_dir
        if args.output_dir is not None
        else os.path.dirname(args.index_path)
    )
    os.makedirs(args.output_dir, exist_ok=True)

    logger = logs.get_logger("news")
    logger.info(f"Downloading pages found in {args.index_path}")
    with open(args.index_path) as f:
        page_index = [json.loads(line) for l in f if (line := l.strip())]

    if not page_index:
        logger.error(f"{args.index_path} is empty.")
        raise ValueError(f"{args.index_path} is empty.")

    if args.test_run:
        logger.info(f"Test Run, only downloading {args.test_run} random pages.")
        random.seed(42)
        random.shuffle(page_index)
        page_index = page_index[: args.test_run]

    # Download all pages
    # We don't process the results, they are just written to disk, so we
    # use map to make sure it actually gets run.
    logger.info(f"Saving pages to {args.output_dir}")
    with mp.Pool(args.num_workers) as p:
        _ = p.map(
            functools.partial(
                get_pages,
                output_dir=args.output_dir,
                overwrite=args.overwrite,
                wait=args.wait,
                dry_run=args.dry_run,
            ),
            page_index,
        )


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("news")
    main(args)
