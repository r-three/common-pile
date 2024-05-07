"""Build out a list of all pages on a website based on their sitemap."""

import argparse
import json
import os

import utils

from licensed_pile import logs, scrape

parser = argparse.ArgumentParser(description="Find all pages on a news site.")
parser.add_argument("--url", required=True, help="Base URL")
parser.add_argument(
    "--index_path",
    required=True,
    help="File that list of all pages",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Create a new index.",
)


def main(args):
    logger = logs.get_logger("news")
    if os.path.exists(args.index_path) and not args.overwrite:
        logger.error(f"Page Index already exists at {args.index_path}, aborting.")
        return
    logger.info(f"Building page index from {args.url}")
    page_list = utils.build_url_index(args.url)
    page_list = sorted(set(page_list))
    logger.info(f"Found {len(page_list)} pages.")
    page_index = [
        {"idx": idx, "url": url, "filename": f"{utils.url_to_filename(url)}.html"}
        for idx, url in enumerate(page_list)
    ]
    logger.info(f"Saving page index to {args.index_path}")
    os.makedirs(os.path.dirname(args.index_path), exist_ok=True)
    with open(args.index_path, "w") as wf:
        wf.write("\n".join(json.dumps(p) for p in page_index) + "\n")


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("news")
    main(args)
