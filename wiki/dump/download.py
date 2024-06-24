"""Download and extract official wiki dumps."""

import argparse
import os
import re
import urllib.parse

import pyunpack

from licensed_pile import logs, scrape

parser = argparse.ArgumentParser(
    description="Download and Extract official Wiki dumps."
)
parser.add_argument("--url", help="The url to download a dump from.")
parser.add_argument("--wikimedia", help="")
parser.add_argument(
    "--output_dir", default="data/dumps", help="Where to save the downloaded dumps."
)


def wikimedia_url(wikimedia):
    wikimedia = re.sub(r"^en", "", wikimedia)
    return f"https://dumps.wikimedia.org/en{wikimedia}/latest/en{wikimedia}-latest-pages-articles-multistream.xml.bz2"


def download_and_extract(url, ident, output_dir):
    filename = os.path.basename(urllib.parse.urlparse(url).path)


def main(args):
    if args.url and args.wikimedia:
        raise ValueError(
            f"--url={args.url} and --wikimedia={args.wikimedia} cannot be set at the same time."
        )
    if not (args.url or args.wikimedia):
        raise ValueError(f"--url or --wikimedia must be set.")
    if not args.url:
        args.url = wikimedia_url(args.wikimedia)

    ident = ...
    download_and_extract(args.url, ident, args.output_dir)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("wiki/dump")
    main(args)
