"""Convert the site list xml to download urls."""

import argparse
import functools
import os
import sys
import urllib.parse
import xml.etree.ElementTree as ET

from bs4 import BeautifulSoup

ARCHIVE_ORG_URL = "https://archive.org/download/stackexchange/"


parser = argparse.ArgumentParser(description="Generate a list of urls to download.")
parser.add_argument(
    "--sites", default="data/Sites.xml", help="The path to the Sites.xml file."
)
parser.add_argument(
    "--format",
    default="xml",
    choices=["xml", "index"],
    help="What format does the site list come in.",
)
parser.add_argument(
    "--base_url",
    default=ARCHIVE_ORG_URL,
    help="What is the base URL of the archive dump?",
)


def to_download(site, base_url: str = ARCHIVE_ORG_URL):
    site = f"{site}.7z" if not site.endswith(".7z") else site
    return urllib.parse.urljoin(base_url, site)


def parse_xml(sites):
    tree = ET.parse(sites)
    root = tree.getroot()
    urls = (child.attrib["Url"] for child in root)
    yield from (urllib.parse.urlparse(url).netloc for url in urls)


def parse_index(sites):
    with open(sites) as f:
        soup = BeautifulSoup(f, features="lxml")
    table = soup.find("table", class_="directory-listing-table")
    body = table.find("tbody")
    for row in body.find_all("tr"):
        cell = row.find("td")
        link = cell.find("a")
        if link.get("href", "") in ("../", "LICENSE.txt", "README.md"):
            continue
        yield link["href"]


def main(args):
    if args.format == "xml":
        parse_urls = parse_xml
    elif args.format == "index":
        parse_urls = parse_index
    else:
        raise ValueError(f"Unknown site format, got --format {args.format}")

    urls = parse_urls(args.sites)
    dl = map(functools.partial(to_download, base_url=args.base_url), urls)
    for link in dl:
        # Write to stdout so this is pipe-able, see the Python docs on handling
        # broken pipes from things like head.
        # https://docs.python.org/3/library/signal.html#note-on-sigpipe
        try:
            sys.stdout.write(link + "\n")
            sys.stdout.flush()
        except BrokenPipeError:
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
            sys.exit(1)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
