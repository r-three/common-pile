"""Scrape DPR data."""

import argparse
import datetime
import functools
import itertools
import json
import multiprocessing.dummy as mp
import os
import re
import textwrap
import time
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from utils import (
    BASE_URL,
    SOURCE_NAME,
    contains_permissive_license,
    get_content,
    get_elements,
    get_elements_text,
    get_outbound_links,
    parse_date,
)

from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.utils import removeprefix
from licensed_pile.write import to_dolma


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_dir",
        default=f"data/{SOURCE_NAME}/v0",
        help="Where the dolma formatted data goes",
    )
    parser.add_argument(
        "--filename",
        default=None,
        help="The base filename for the PDR data",
    )
    parser.add_argument(
        "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
    )
    parser.add_argument(
        "--test_run",
        type=int,
        default=None,
        help="Number to scrape when running test code.",
    )
    parser.add_argument(
        "--type",
        choices=["collection", "essay", "conjecture"],
        default="collection",
        help="Which type of data are we scraping?",
    )
    parser.add_argument(
        "--num_threads",
        type=int,
        default=16,
        help="The number of threads to use when downloading the data.",
    )
    parser.add_argument(
        "--base_url", default=BASE_URL, help="The root url we are scraping from."
    )
    parser.add_argument(
        "--wait",
        type=int,
        default=2,
        help="How long to wait between requests in a thread to reduce server load.",
    )

    args = parser.parse_args()
    return args


def generate_example_links(starting_url, example_regex, pagination_regex):
    logger = logs.get_logger("public-domain-review")
    pages = {starting_url}
    seen_pages = set()
    seen_links = set()
    while pages:
        page = pages.pop()
        logger.info(f"Scraping {page} to find links to examples.")
        html = get_content(page)
        if html:
            links = get_outbound_links(BeautifulSoup(html, "html.parser"), page)

            essay_links = [link for link in links if example_regex.match(link)]
            for link in essay_links:
                if link in seen_links:
                    continue
                logger.info(f"Found {link} on {page}")
                yield link
                seen_links.add(link)

            seen_pages.add(page)
            page_links = [
                link
                for link in links
                if pagination_regex.match(link) and link not in seen_pages
            ]
            pages.update(page_links)


def parse_collection_html(html):
    document = BeautifulSoup(html, "html.parser")

    header = get_elements(document, "div", "collection-header")[0]
    title = get_elements_text(header, "h1", None)[0]
    byline = get_elements_text(document, "div", "attribution")[0]

    intro = get_elements_text(document, "p", "intro")[0]
    date = get_elements_text(document, "p", "date")[0]

    text_blocks = "\n".join(get_elements_text(document, "div", "essay__text-block"))

    text = (
        textwrap.dedent(
            """
    {title}
    {byline}
    {date}

    {intro}

    {text_blocks}
    """
        )
        .strip()
        .format(
            title=title, byline=byline, date=date, intro=intro, text_blocks=text_blocks
        )
    )

    author = removeprefix(byline, "Text by ")
    return author, date, text


def parse_essay_html(html):
    document = BeautifulSoup(html, "html.parser")
    essay = get_elements(document, "div", "essay-view")[0]

    title = get_elements_text(essay, "span", "title")[0]
    subtitle = get_elements_text(essay, "span", "subtitle")[0]
    byline = get_elements_text(essay, "p", "byline")[0]

    intro = get_elements_text(essay, "p", "intro")[0]
    date = get_elements_text(essay, "p", "date")[0]

    text_blocks = "\n".join(get_elements_text(essay, "div", "essay__text-block"))

    text = (
        textwrap.dedent(
            """
    {title}
    {subtitle}
    {byline}
    {date}

    {intro}

    {text_blocks}
    """
        )
        .strip()
        .format(
            title=title,
            subtitle=subtitle,
            byline=byline,
            date=date,
            intro=intro,
            text_blocks=text_blocks,
        )
    )
    author = removeprefix(byline, "By ")
    return author, date, text


def make_record(
    link,
    example_type: str,
    parse_example,
    source_name: str = SOURCE_NAME,
    wait: int = 0,
):
    html = get_content(link)
    author, date, essay = parse_example(html)
    # Add some delay so we don't hammer their servers.
    if wait:
        time.sleep(wait)
    return {
        "id": link.strip("/").split("/")[-1],
        "text": essay,
        "source": source_name,
        "created": parse_date(date),
        "added": datetime.datetime.utcnow().isoformat(),
        "metadata": {
            "license": str(PermissiveLicenses.CC_BY_SA),
            "url": link,
            "type": example_type,
            "author": author,
        },
    }


def main(args):
    args.filename = (
        args.filename if args.filename is not None else f"{args.type}s.jsonl.gz"
    )
    if args.type == "collection":
        starting_url = urljoin(args.base_url, "collections")
        example_regex = re.compile(urljoin(args.base_url, "collection/[a-z0-9-]+/"))
        pagination_regex = re.compile(urljoin(args.base_url, "collections/all/[0-9]+/"))
        parse_example = parse_collection_html
    elif args.type == "essay":
        starting_url = urljoin(args.base_url, "essays")
        example_regex = re.compile(urljoin(args.base_url, "essay/[a-z0-9-]+/"))
        pagination_regex = re.compile(urljoin(args.base_url, "essays/[0-9]+/"))
        parse_example = parse_essay_html
    elif args.type == "conjecture":
        starting_url = urljoin(args.base_url, "series/conjectures")
        example_regex = re.compile(urljoin(args.base_url, "essay/[a-z0-9-]+/"))
        pagination_regex = re.compile(urljoin(args.base_url, "essays/[0-9]+/"))
        parse_example = parse_essay_html
    else:
        raise ValueError(f"{args.type} not understood.")

    logger = logs.get_logger("public-domain-review")

    logger.info(f"Fetching {args.type} examples links from {starting_url}")
    # Collect all the links in a single thread.
    links = generate_example_links(starting_url, example_regex, pagination_regex)
    if args.test_run:
        logger.info(f"Test run, only scraping {args.test_run} examples.")
        links = itertools.islice(links, args.test_run)
    # Using threads is ok because I think we will be I/O bound most of the time.
    logger.info(f"Scraping and formatting examples using {args.num_threads} threds.")
    with mp.Pool(args.num_threads) as pool:
        records = pool.imap(
            functools.partial(
                make_record,
                example_type=args.type,
                parse_example=parse_example,
                wait=args.wait,
            ),
            links,
        )
        to_dolma(records, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("public-domain-review")
    main(args)
