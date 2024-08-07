"""This uses the list pages from https://wikiindex.org to get the urls of all wikis."""

import argparse
import functools
import glob
import multiprocessing.dummy as mp
import os
import time
import urllib.parse
from typing import List

import tenacity
from utils import enumerate_pages, get_page, get_soup, get_wiki_name, make_wiki_url

from licensed_pile import logs

parser = argparse.ArgumentParser(description="Convert a list of wikinames to urls.")
parser.add_argument(
    "--wiki", default="https://wikiindex.org", help="The wiki url we are exporting."
)
parser.add_argument(
    "--pages",
    action="append",
    help="A list of files of pages to export, or a dir to export all. defaults to data/${wiki_name}/pages/.",
)
parser.add_argument(
    "--test_pages", default=None, type=int, help="The number of test pages to retrieve."
)
parser.add_argument(
    "--output",
    help="Where to save the output. defaults to data/${wiki_name}/export/.",
)
# Using const="" allows us to have the empty string be the value used when
# only --prefix is passed.
parser.add_argument(
    "--wiki_prefix",
    default="",
    nargs="?",
    const="",
    help="Prefix for url paths, changes between wiki's, often wiki/, w/, or nothing (just pass --wiki_prefix)",
)
parser.add_argument(
    "--num_threads",
    default=64,
    type=int,
    help="The number of threads to use when fetching wiki names, as this is I/O blocking, you can use more than your core count without much issue.",
)
parser.add_argument(
    "--wait", default=2, type=int, help="How long to wait between requests."
)


def get_wiki_link(
    page_title: str, wiki_url: str, url_prefix: str = "", wait: int = 0
) -> str:
    url = make_wiki_url(wiki_url, page_title, url_prefix)
    logger = logs.get_logger("wiki.scrape")
    logger.info(f"Finding external link to {url}")
    try:
        soup = get_soup(get_page(url))
        ext_url = get_external_link(soup)
        time.sleep(wait)
        logger.info(f"Found {ext_url} as the external url for {url}")
        return ext_url
    except tenacity.RetryError:
        logger.error(f"Failed to fetch {url}")


def get_external_link(soup) -> str:
    if ext_link := soup.find("a", {"class": "external text"}):
        return ext_link.attrs["href"]


def main(args):
    logger = logs.get_logger("wiki.scrape")
    args.pages = (
        args.pages
        if args.pages is not None
        else [os.path.join("data", get_wiki_name(args.wiki), "pages")]
    )
    args.output = (
        args.output
        if args.output is not None
        else os.path.join("data", get_wiki_name(args.wiki), "wiki_list.txt")
    )
    logger.info(f"Enumerating pages from {args.pages}")
    pages = enumerate_pages(args.pages)
    logger.info(f"There are {len(pages)} wikis")

    # Only fetch some for testing.
    pages = pages[: args.test_pages]

    logger.info(f"Fetching wiki links with {args.num_threads} threads.")
    with mp.Pool(args.num_threads) as pool:
        links = pool.map(
            functools.partial(
                get_wiki_link,
                wiki_url=args.wiki,
                url_prefix=args.wiki_prefix,
                wait=args.wait,
            ),
            pages,
        )

        logger.info(f"Writing wiki links to {args.output}")
        with open(args.output, "w") as wf:
            wf.write("\n".join(filter(lambda l: l is not None, links)) + "\n")


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("wiki.scrape")
    main(args)
