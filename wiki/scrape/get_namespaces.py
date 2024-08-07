"""Enumerate all the namespaces in a mediawiki wiki."""

import argparse
import json
import os
import urllib.parse
from typing import Dict

from utils import get_page, get_soup, get_wiki_name, removesuffix

from licensed_pile import logs

parser = argparse.ArgumentParser(description="Find all namespaces in a mediawiki wiki.")
parser.add_argument("--wiki", required=True, help="The Url for the wiki in question.")
parser.add_argument(
    "--output",
    help="Where to save the id -> namespace mapping. Normally (data/${wiki_name}/namespaces.json)",
)
# Using const="" allows us to have the empty string be the value used when
# only --prefix is passed.
parser.add_argument(
    "--wiki_prefix",
    default="wiki/",
    nargs="?",
    const="",
    help="Prefix for url paths, changes between wiki's, often wiki/, w/, or nothing (just pass --wiki_prefix)",
)


def find_namespaces(wiki_url: str, url_prefix: str = "/wiki/") -> Dict[int, str]:
    options = {}
    # Even though they recommend using the index.php?title=PAGETITLE url for a lot
    # of things (with the /wiki/ being for readers), we use it here to start looking
    # for pages because it is more consistent (some wiki's want /w/index.php and
    # some just want /index.php).
    # TODO: Code would probably be able to automatically try and select the prefix
    # by trying each of the common ones.
    # Normalize the prefix by removing any trailing slash.
    url_prefix = removesuffix(url_prefix, "/")
    soup = get_soup(
        get_page(urllib.parse.urljoin(wiki_url, f"{url_prefix}/Special:AllPages"))
    )
    # Extract the list of namespaces from the URL
    namespaces = soup.find(id="namespace")
    for option in namespaces.find_all("option"):
        options[option.text] = int(option.attrs["value"])
    return options


def main(args):
    logger = logs.get_logger("wiki.scrape")
    logger.info(f"Finding all namespaces from {args.wiki}")
    namespaces = find_namespaces(args.wiki, args.wiki_prefix)
    args.output = (
        args.output
        if args.output is not None
        else os.path.join("data", get_wiki_name(args.wiki), "namespaces.json")
    )

    os.makedirs(os.path.dirname(args.output), exist_ok=True)
    logger.info(f"Writing namespaces to {args.output}")
    with open(args.output, "w") as wf:
        json.dump(namespaces, wf, indent=2)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("wiki.scrape")
    main(args)
