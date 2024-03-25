import argparse
from urllib.parse import urljoin
import re
import logging
import os
import json
import textwrap

import requests
from bs4 import BeautifulSoup


logging.basicConfig(level=logging.INFO, format="scrape: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", required=True, help="Path to save parsed essays to")
    args = parser.parse_args()
    return args


def get_outbound_links(url, html):
    soup = BeautifulSoup(html, "html.parser")
    links = soup.find_all("a")
    outbound_links = [urljoin(url, link.get('href')) for link in links if link.get('href')]
    return set(outbound_links)


def get_content(url):
    logging.info(f"Downloaded {url}")
    response = requests.get(url)
    if response.status_code == 200:
        return response.content
    logging.error(f"{url} returned status {response.status_code}")
    return None


def generate_essay_links(args):
    base_url = 'https://publicdomainreview.org/'
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
            page_links = [link for link in links if page_url_pattern.match(link) and link not in seen_pages]
            pages.update(page_links)


def contains_permissive_license(html):
    soup = BeautifulSoup(html, "html.parser")
    license_statement = soup.find_all("div", class_="essay-license essay__content")[0].get_text()
    return "CC BY-SA" in license_statement


def get_elements(bs_obj, element_type, class_name):
    return bs_obj.find_all(element_type, class_=class_name) 


def get_elements_text(bs_obj, element_type, class_name):
    elements = get_elements(bs_obj, element_type, class_name)
    elements_text = [element.get_text().strip() for element in elements]
    if len(elements_text) == 0:
        elements_text = [""]
    return elements_text


def parse_essay_html(html):
    document = BeautifulSoup(html, "html.parser")
    essay = get_elements(document, "div", "essay-view")[0]
    
    title = get_elements_text(essay, "span", "title")[0]
    subtitle = get_elements_text(essay, "span", "subtitle")[0]
    byline = get_elements_text(essay, "p", "byline")[0]

    intro = get_elements_text(essay, "p", "intro")[0]
    date = get_elements_text(essay, "p", "date")[0]
    
    text_blocks = "\n".join(get_elements_text(essay, "div", "essay__text-block"))

    text = textwrap.dedent(
    """
    {title}
    {subtitle}
    {byline}
    {date}

    {intro}
    
    {text_blocks}
    """
    ).strip().format(**locals())
    return text


def main(args):
    essays = {}
    for link in generate_essay_links(args):
        essay_html = get_content(link)
        if contains_permissive_license(essay_html):
            essay = parse_essay_html(essay_html)
            essays[link] = essay

    with open(os.path.join(args.output_dir, "conjectures.json"), "w") as f:
        json.dump(essays, f, indent=4)


if __name__ == "__main__":
    args = parse_args()
    main(args)
