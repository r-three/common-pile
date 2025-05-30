"""Download pages.

This script is separated from the to-dolma script to facilitate easy incremental
downloads (by not passing `--overwrite`, pages that are already downloaded will
be skipped).
"""

import argparse
import functools
import json
import multiprocessing.dummy as mp
import os
import time

from common_pile import logs, scrape

parser = argparse.ArgumentParser(description="Download pages based on the index.")
parser.add_argument(
    "--index_path",
    default="data/pages/page_index.jsonl",
    help="The list of pages to download.",
)
parser.add_argument(
    "--output_dir",
    help="Where to store pages when they are downloaded.",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we re-download and overwrite pages we have already downloaded?",
)
parser.add_argument(
    "--num_threads",
    type=int,
    default=64,
    help="The number of threads to use when downloaded, mostly I/O bound so threads are ok.",
)
parser.add_argument(
    "--test_run",
    type=int,
    help="Test the code by only downloading `--test_run N` documents.",
)
parser.add_argument(
    "--wait",
    type=int,
    default=2,
    help="Time to wait between requests on a single thread.",
)


def download_page(page_info, output_dir, overwrite: bool = True, wait: int = 0):
    """Download the page and save it to disk, unless it is already there."""
    logger = logs.get_logger("food")
    page_path = os.path.join(output_dir, page_info["filename"])

    if not overwrite and os.path.exists(page_path):
        logger.info(f"{page_path} already exists, not downloading {page_info['url']}")
        return
    try:
        logger.info(f"Downloading {page_info['url']}")
        page = scrape.get_page(page_info["url"])
        with open(page_path, "wb") as f:
            f.write(page.content)
    except Exception as err:
        logger.error(f"Failed to fetch {page_info['url']}: {err}")
    if wait:
        # time.sleep releases the GIL so we can use it in threads.
        time.sleep(wait)


def main(args):
    args.output_dir = (
        args.output_dir
        if args.output_dir is not None
        else os.path.dirname(args.index_path)
    )
    os.makedirs(args.output_dir, exist_ok=True)

    logger = logs.get_logger("food")
    logger.info(f"Downloading pages found in {args.index_path}")
    with open(args.index_path) as f:
        page_index = [json.loads(l) for l in f]

    if args.test_run:
        logger.info(f"Test Run, only downloading {args.test_run} pages.")
        page_index = page_index[: args.test_run]

    # Download all the pages
    # We don't process the results, just write them to disk, so we use map over
    # imap to ensure it actually gets run.
    # Downloading pages is mostly I/O bound so we use threads.
    logger.info(f"Saving pages to {args.output_dir}")
    with mp.Pool(args.num_threads) as pool:
        _ = pool.map(
            functools.partial(
                download_page,
                output_dir=args.output_dir,
                overwrite=args.overwrite,
                wait=args.wait,
            ),
            page_index,
        )


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("food")
    main(args)
