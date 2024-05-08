"""Build a list of pages to scape based on the sitemap."""


import argparse
import json
import os
import re
from typing import List

import usp.tree

from licensed_pile import logs, scrape, utils

parser = argparse.ArgumentParser(
    description="Find all pages to download based on the sitemap."
)
parser.add_argument(
    "--url", default="https://www.foodista.com/", help="The site we are scraping."
)
parser.add_argument(
    "--index_path",
    default="data/pages/page_index.jsonl",
    help="Where to save the list of pages.",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite a previous list of pages we made?",
)


def build_url_index(url: str) -> List[str]:
    logs.configure_logging("usp.helpers")
    logs.configure_logging("usp.fetch_parse")
    logs.configure_logging("usp.tree")

    tree = usp.tree.sitemap_tree_for_homepage(url)
    page_list = sorted(set(page.url for page in tree.all_pages()))
    # Remove homepage and the _ping healthcheck page.
    page_list = page_list[2:]
    return page_list


def url_to_filename(url: str) -> str:
    url = re.sub(r"https?://(?:www\.)?", "", url)
    url = re.sub(r"[?,=/]", "_", url)
    url = re.sub(r"\s+", "_", url)
    return url


def main(args):
    logger = logs.get_logger("food")
    if os.path.exists(args.index_path) and not args.overwrite:
        logger.error(f"Page Index already exists at {args.index_path}, aborting.")
        return
    logger.info(f"Building page index from {args.url}")
    page_list = build_url_index(args.url)
    logger.info(f"Found {len(page_list)} pages.")
    page_index = [
        {"idx": idx, "url": url, "filename": f"{url_to_filename(url)}.html"}
        for idx, url in enumerate(page_list)
    ]
    logger.info(f"Saving page index to {args.index_path}")
    os.makedirs(os.path.dirname(args.index_path), exist_ok=True)
    with open(args.index_path, "w") as wf:
        wf.write("\n".join(json.dumps(p) for p in page_index) + "\n")


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("food")
    main(args)
