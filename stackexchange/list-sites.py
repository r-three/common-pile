"""Convert the site list xml to download urls."""

import argparse
import os
import sys
import urllib.parse
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser(description="Generate a list of urls to download.")
parser.add_argument(
    "--sites", default="data/Sites.xml", help="The path to the Sites.xml file."
)


def to_download(url, base_url: str = "https://archive.org/download/stackexchange/"):
    loc = urllib.parse.urlparse(url).netloc
    return urllib.parse.urljoin(base_url, f"{loc}.7z")


def main(args):
    tree = ET.parse(args.sites)
    root = tree.getroot()
    urls = (child.attrib["Url"] for child in root)
    dl = map(to_download, urls)
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
