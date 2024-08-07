"""Create a list of all pages under a namespace for a mediawiki."""

import argparse
import json
import os
import urllib.parse
from typing import List

from requests.models import PreparedRequest
from utils import get_page, get_soup, get_wiki_name, removeprefix, removesuffix

from licensed_pile import logs

parser = argparse.ArgumentParser(
    description="Find all pages under a namespace for a mediawiki."
)
parser.add_argument("--wiki", required=True, help="The Url for the wiki in question.")
parser.add_argument(
    "--namespace",
    "-ns",
    required=True,
    action="append",
    help="The namespace to enumerate.",
)
parser.add_argument(
    "--namespace_map", help="The id -> namespace mapping file, stored as json."
)
parser.add_argument("--output_dir", help="Where to store the list of file outputs.")
# Using const="" allows us to have the empty string be the value used when
# only --prefix is passed.
parser.add_argument(
    "--wiki_prefix",
    default="wiki/",
    nargs="?",
    const="",
    help="Prefix for url paths, changes between wiki's, often wiki/, w/, or nothing (just pass --wiki_prefix)",
)


def enumerate_namespace(
    wiki_url: str, namespace: int, url_prefix: str = "wiki/"
) -> List[str]:
    """Collect all pages of a wiki from within a namespace."""
    logger = logs.get_logger("wiki.scrape")
    logger.info(f"Finding all pages under the {namespace} namespace from {wiki_url}")
    # Even though they recomment using the index.php?title=PAGETITLE url for a lot
    # of things (with the /wiki/ being for readers), we use it here to start looking
    # for pages because it is more consistent (some wiki's want /w/index.php and
    # some just want /index.php).
    # TODO: Code would probably be able to automatically try and select the prefix
    # by trying each of the common ones.
    # Normalize the prefix by removing any trailing slash.
    url_prefix = removesuffix(url_prefix, "/")
    url = urllib.parse.urljoin(wiki_url, f"{url_prefix}/Special:AllPages")
    # Use a prepared request to build out the url with parameters, we don't
    # actually use this to make requests.
    r = PreparedRequest()
    r.prepare_url(url, {"namespace": namespace, "hideredirects": "1"})
    return _enumerate_namespace(
        r.url,
        wiki_url,
        [],
    )


def _enumerate_namespace(url: str, wiki_url: str, pages: List[str]) -> List[str]:
    """Collect all pages of a wiki from within a namespace.

    Args:
      url: The current pagination URL to get the next set of links from.
      wiki_url: The base url as pagination links don't include the host information.
        Note: If we move this recurrent function to an inner function of the above
              this wouldn't need to be a parameter.
      pages: The current list of pages we are building.
    """
    logger = logs.get_logger("wiki.scrape")
    logger.info(f"Finding page links in {url}")
    soup = get_soup(get_page(url))
    # Find all the links in the page
    page_count = len(pages)
    # Sometimes this is in a div and sometimes it is in a table, so just look for class names.
    if links := soup.find(True, {"class": ["mw-allpages-body", "mw-allpages-chunk"]}):
        for link in links.find_all("a"):
            href = link.attrs["href"]
            href = removeprefix(href, "/")
            href = removeprefix(href, "wiki/")
            pages.append(urllib.parse.unquote(href))
    logger.info(f"Found {len(pages) - page_count} pages")

    # Find a pagination link
    if nav := soup.find("div", {"class": "mw-allpages-nav"}):
        for link in nav.find_all("a"):
            # Pagination links look like "Next Page (${page title})"
            # Check the start of the text to make sure we don't find a link that has
            # a title that contains "next page".
            if link.text.lower().startswith("next page"):
                # Recurse using the pagination link as the new url.
                try:
                    logger.info(f"Found pagination page at {link.attrs['href']}")
                    # The current page links have already been added to pages so we can
                    # just return whatever the recusion gives us.
                    return _enumerate_namespace(
                        urllib.parse.urljoin(wiki_url, link.attrs["href"]),
                        wiki_url,
                        pages,
                    )
                except Exception as e:
                    # If something goes wrong in pagination, just return the pages we
                    # have.
                    logger.info(
                        f"Something went wrong processing pagination at {link.attrs['href']}, returning partial results."
                    )
                    return pages
    # If no pagination link was found, just return what we have.
    logger.info(f"No pagination link found, finished.")
    return pages


def main(args):
    args.namespace_map = (
        args.namespace_map
        if args.namespace_map is not None
        else os.path.join("data", get_wiki_name(args.wiki), "namespaces.json")
    )
    with open(args.namespace_map) as f:
        namespace_map = json.load(f)
    args.output_dir = (
        args.output_dir
        if args.output_dir is not None
        else os.path.join("data", get_wiki_name(args.wiki), "pages")
    )
    os.makedirs(args.output_dir, exist_ok=True)

    for namespace in args.namespace:
        # Convert to int using map if it was a string, otherwise default keeps it as int.
        namespace = namespace_map.get(namespace, namespace)
        pages = enumerate_namespace(args.wiki, namespace, args.wiki_prefix)
        with open(os.path.join(args.output_dir, f"{namespace}.txt"), "w") as wf:
            wf.write("\n".join(pages) + "\n")


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("wiki.scrape")
    main(args)
