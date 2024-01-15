"""Utilities for scraping wikis."""

import glob
import os
import urllib.parse
from typing import Dict, List, Optional

import requests
from bs4 import BeautifulSoup

from licensed_pile import scrape


def get_page(*args, **kwargs):
    r = scrape.get_page(*args, **kwargs)
    return r.text


def get_soup(text, parser="html.parser"):
    """Abstract into a function in case we want to swap how we parse html."""
    return BeautifulSoup(text, parser)


def get_wiki_name(url: str) -> str:
    """Use a wiki's url as it's name.

    This functions is to abstract into a semantic unit, even though it doesn't do much.
    """
    return urllib.parse.urlparse(url).netloc


# We don't use snake case as the string methods added in PIP616 are named like this.
def removeprefix(s: str, prefix: str) -> str:
    """In case we aren't using python >= 3.9"""
    if s.startswith(prefix):
        return s[len(prefix) :]
    return s[:]


# We don't use snake case as the string methods added in PIP616 are named like this.
def removesuffix(s: str, suffix: str) -> str:
    """In case we aren't using python >= 3.9"""
    # Check for suffix to avoid calling s[:-0] for an empty string.
    if suffix and s.endswith(suffix):
        return s[: -len(suffix)]
    return s[:]


def make_wiki_url(base_url: str, title: str, url_prefix: str = "wiki/") -> str:
    """Create a wiki url from the wiki url and the page name."""
    url_prefix = removesuffix(url_prefix, "/")
    url = urllib.parse.urljoin(base_url, f"{url_prefix}/{title.replace(' ', '_')}")
    return urllib.parse.quote(url, safe=":/")


def read_page_titles(filename: str) -> List[str]:
    with open(filename) as f:
        return f.read().strip("\n").split("\n")


def enumerate_pages(pages: List[str], pattern: str = "*.txt") -> List[str]:
    """Enumerate all pages found in a wiki scrape.

    Args:
      pages: A list of paths to text files containing one page title per line or
        a dir containing multiple page files.
      pattern: A glob pattern to find page files within pages[i] when it is a dir.
    """
    results = []
    for page in pages:
        if os.path.exists(page) and os.path.isdir(page):
            for f in glob.iglob(os.path.join(page, pattern)):
                results.extend(read_page_titles(f))
        else:
            results.extend(read_page_titles(page))
    return results
