"""Utilities for scraping DPR data."""

import datetime
import logging
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from licensed_pile import logs
from licensed_pile.scrape import get_page

BASE_URL = "https://publicdomainreview.org/"
SOURCE_NAME = "public-domain-review"


def get_outbound_links(soup, url: str = ""):
    """Find all links in a webpage.

    Args:
      soup: The parsed webpage.
      url: A base url, if a link is an absolute link, urljoin will return just
        that link, if it is relative, it will be added to this base url.
    """
    links = soup.find_all("a")
    outbound_links = [
        urljoin(url, link.get("href")) for link in links if link.get("href")
    ]
    return set(outbound_links)


def get_content(url):
    """Download a url from the internet, returns None if there is an error."""
    logger = logs.get_logger("public-domain-review")
    logger.info(f"Downloading {url}")
    try:
        response = get_page(url)
        return response.content
    except RuntimeError:
        logger.error(f"Failed to download {url}")
        return None


def contains_permissive_license(soup):
    """Check if a page has the CC by-sa license."""
    license_statement = get_elements_text(soup, "div", "essay-license essay__content")[
        0
    ]
    return "CC BY-SA" in license_statement


def get_elements(soup, element_type, class_name):
    """A nicer interface to finding elements, returns [] when not found."""
    return soup.find_all(element_type, class_=class_name)


def get_elements_text(soup, element_type, class_name):
    """Get the text from each element, return [""] if no elements are found."""
    elements = get_elements(soup, element_type, class_name)
    elements_text = [element.get_text().strip() for element in elements]
    if not elements_text:
        elements_text = [""]
    return elements_text


def parse_date(date: str) -> datetime.datetime:
    """Parse a date into a datetime object."""
    date_formats = ["%B %d, %Y", "%b %d, %Y"]
    for fmt in date_formats:
        try:
            return datetime.datetime.strptime(date, fmt).isoformat()
        except:
            pass
    logger = logs.get_logger("public-domain-review")
    logger.warning(f"Filed to parse date: {date}")
    return date
