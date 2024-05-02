import logging
import requests
from urllib.parse import urljoin

from bs4 import BeautifulSoup


SOURCE_NAME = "public-domain-review"


def get_outbound_links(url, html):
    soup = BeautifulSoup(html, "html.parser")
    links = soup.find_all("a")
    outbound_links = [
        urljoin(url, link.get("href")) for link in links if link.get("href")
    ]
    return set(outbound_links)


def get_content(url):
    logging.info(f"Downloaded {url}")
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    logging.error(f"{url} returned status {response.status_code}")
    return None


def contains_permissive_license(html):
    soup = BeautifulSoup(html, "html.parser")
    license_statement = soup.find_all("div", class_="essay-license essay__content")[
        0
    ].get_text()
    return "CC BY-SA" in license_statement


def get_elements(bs_obj, element_type, class_name):
    return bs_obj.find_all(element_type, class_=class_name)


def get_elements_text(bs_obj, element_type, class_name):
    elements = get_elements(bs_obj, element_type, class_name)
    elements_text = [element.get_text().strip() for element in elements]
    if len(elements_text) == 0:
        elements_text = [""]
    return elements_text
