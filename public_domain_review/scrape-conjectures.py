import argparse
from urllib.parse import urljoin
import re
import logging
import os
import json
import textwrap
import datetime

import requests
from bs4 import BeautifulSoup

from utils import (
    SOURCE_NAME,
    get_outbound_links,
    get_content,
    contains_permissive_license,
    get_elements,
    get_elements_text,
)
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma


logging.basicConfig(
    level=logging.INFO,
    format="scrape-conjectures: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s",
)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-dir",
        default=f"data/{SOURCE_NAME}/v0",
        help="Where the dolma formatted data goes",
    )
    parser.add_argument(
        "--filename",
        default=f"conjectures.jsonl.gz",
        help="The base filename for the PDR data",
    )
    parser.add_argument(
        "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
    )

    args = parser.parse_args()
    return args


def generate_essay_links(args):
    base_url = "https://publicdomainreview.org/"
    essay_url = urljoin(base_url, "series/conjectures")
    essay_url_pattern = re.compile(urljoin(base_url, "essay/[a-z0-9-]+/"))
    page_url_pattern = re.compile(urljoin(base_url, "essays/[0-9]+/"))

    pages = set([essay_url])
    seen_pages = set()
    seen_links = set()
    while len(pages) > 0:
        page = pages.pop()
        logging.info(f"Scraping {page}")
        html = get_content(page)
        if html:
            links = get_outbound_links(page, html)

            essay_links = [link for link in links if essay_url_pattern.match(link)]
            for link in essay_links:
                if link in seen_links:
                    continue
                logging.info(f"Found {link}")
                yield link
                seen_links.add(link)

            seen_pages.add(page)
            page_links = [
                link
                for link in links
                if page_url_pattern.match(link) and link not in seen_pages
            ]
            pages.update(page_links)


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
        .format(**locals())
    )
    author = byline.strip("By ")
    return author, date, text


def generate_records(args):
    for link in generate_essay_links(args):
        essay_html = get_content(link)
        if contains_permissive_license(essay_html):
            author, date, essay = parse_essay_html(essay_html)
            yield {
                "id": link.strip("/").split("/")[-1],
                "text": essay,
                "date": date,
                "author": author,
                "source": SOURCE_NAME,
                "type": "conjecture",
                "added": datetime.datetime.utcnow().isoformat(),
                "metadata": {"license": str(PermissiveLicenses.CC_BY_SA), "url": link},
            }


def main(args):
    records = generate_records(args)
    to_dolma(records, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parse_args()
    main(args)
