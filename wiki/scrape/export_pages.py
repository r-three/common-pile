"""Export the pages we enumerated as xml.

This page https://www.mediawiki.org/wiki/Manual:Parameters_to_Special:Export
lists multiple limits to the amount of data that can be returned. In the two
wiki's I have been testing on I haven't found these to be true. The main
points of concern are:
* pages: The limit is 35
* limit: The maximum number of revisions to return, limited at 1000.
* history: It mentions there are cases where this doesn't return all the revisions.
* listauthors: This didn't seem active on any of the wikis I tested on.
"""


import argparse
import os
import urllib.parse
from typing import List

from utils import enumerate_pages, get_page, get_wiki_name

from licensed_pile import logs

parser = argparse.ArgumentParser(description="Export mediawikis as XML")
parser.add_argument("--wiki", required=True, help="The wiki url we are exporting.")
parser.add_argument(
    "--pages",
    action="append",
    help="A list of files of pages to export, or a dir to export all. defaults to data/${wiki_name}/pages/.",
)
# Using firefox I didn't have issues sending a lot of pages at once, but I was
# getting URI too long errors when using requests.
parser.add_argument(
    "--page_limit", default=35, help="The max number of pages to export at once."
)
parser.add_argument(
    "--test_pages", default=None, type=int, help="The number of test pages to retrieve."
)
parser.add_argument(
    "--output_dir",
    help="Where to save the xml export. defaults to data/${wiki_name}/export/.",
)
# TODO: Implement this if we find a wiki that has this enabled.
parser.add_argument(
    "--listauthors",
    help="Use the listauthors url param instead of getting multiple revisions. UNIMPLEMENTED.",
)


def export_pages(wiki: str, pages: List[str]):
    # Note: We don't quote the newline ourselves as requests will do it too and
    # you'll get `%250A` instead of `%0A` in the url.
    pages = "\n".join(pages).strip("\n")
    # Even though they recomment using the index.php?title=PAGETITLE url for a lot
    # of things (with the /wiki/ being for readers), we use it here to start looking
    # for pages because it is more consistent (some wiki's want /w/index.php and
    # some just want /index.php).
    return get_page(
        urllib.parse.urljoin(wiki, "/wiki/Special:Export"),
        params={"pages": pages, "history": 1},
    )


def main(args):
    if args.listauthors is not None:
        raise NotImplementedError("--listauthors is current not implemented.")
    logger = logs.get_logger("wiki.scrape")
    args.pages = (
        args.pages
        if args.pages is not None
        else [os.path.join("data", get_wiki_name(args.wiki), "pages")]
    )
    logger.info("Enumerating pages from %s", args.pages)
    pages = enumerate_pages(args.pages)
    logger.info("There are %d pages to export.", len(pages))

    args.output_dir = (
        args.output_dir
        if args.output_dir is not None
        else os.path.join("data", get_wiki_name(args.wiki), "export")
    )
    os.makedirs(args.output_dir, exist_ok=True)
    logger.info("Saving export to %s", args.output_dir)

    # Save shards of exported pages to
    #   data/${wiki_name}/export/${shard_idx}-pages.xml
    # These shards can be processed as if they are one large xml file with
    #   licensed_pile.xml.iterate_xmls(glob.iglob(...), tag)
    # Note: These exports seem to an xml namespace so all tags are actually
    #   "{http://mediawiki.org/xml/export-0.11/}TAGNAME"
    # with literal "{"'s.
    for i, j in enumerate(range(0, len(pages), args.page_limit)):
        xml = export_pages(args.wiki, pages[j : j + args.page_limit])
        with open(os.path.join(args.output_dir, f"{i:>05}-pages.xml"), "w") as wf:
            wf.write(xml)
        if args.test_pages and j > args.test_pages:
            logger.info(f"Scraped {j + args.page_limit} pages, stopping for testing.")
            break


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("wiki.scrape")
    main(args)
