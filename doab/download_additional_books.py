import argparse
import base64
import glob
import os
import re
import urllib
from urllib.parse import urljoin, urlparse

import magic
import pandas as pd
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm


def extract_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    # Step 1: <a href=...>
    for a in soup.find_all("a", href=True):
        href = a["href"]
        links.add(urljoin(base_url, href))

    # Step 2: Elements with "onclick" that contain a URL
    onclick_pattern = re.compile(r"(https?://[^\s'\";]+)")
    for tag in soup.find_all(attrs={"onclick": True}):
        onclick = tag["onclick"]
        matches = onclick_pattern.findall(onclick)
        for match in matches:
            links.add(urljoin(base_url, match))

    # Step 3a: Check all data-* attributes for embedded URLs
    for tag in soup.find_all(True):
        for attr, value in tag.attrs.items():
            if attr.startswith("data-") and isinstance(value, str):
                # crude heuristic: look for URLs or known extensions
                if "http" in value or value.endswith(".pdf"):
                    links.add(urljoin(base_url, value))

    # Step 3b: Decode base64 values in data-submit attributes
    for tag in soup.find_all(attrs={"data-submit": True}):
        b64_str = tag["data-submit"]
        try:
            decoded = base64.b64decode(b64_str).decode("utf-8")
            if decoded.startswith("/") or decoded.startswith("http"):
                links.add(urljoin(base_url, decoded))
        except Exception:
            pass  # skip if not valid base64

    # Filter out some common false positive patterns
    return list(
        set([l for l in links if "pdf" in l.lower() and "flyer" not in l.lower()])
    )


def is_html(filepath):
    mime = magic.Magic(mime=True)
    return mime.from_file(filepath) == "text/html"


parser = argparse.ArgumentParser(description="Download books from DOAB")
parser.add_argument("metadata", type=str, help="Path to the metadata file")
parser.add_argument("input_glob", type=str, help="Path to already downloaded files")
parser.add_argument("output", type=str, help="Path to the output directory")
args = parser.parse_args()

# Load the metadata file
metadata = pd.read_csv(args.metadata)
metadata = metadata.set_index("id")

# Iterate over files already downloaded
n_bytes = 0
pbar = tqdm(glob.glob(args.input_glob))
for fpath in pbar:
    if is_html(fpath):
        with open(fpath, "r") as f:
            html = f.read()
        id = os.path.splitext(os.path.basename(fpath))[0]
        original_url = metadata.loc[id]["BITSTREAM Download URL"]
        original_url_parsed = urlparse(original_url)
        base_url = f"{original_url_parsed.scheme}://{original_url_parsed.netloc}"

        links = extract_links(html, base_url)
        if len(links) != 1:
            continue

        url = links[0]
        try:
            parsed = urllib.parse.urlsplit(url)
            encoded_path = urllib.parse.quote(parsed.path)
            encoded_query = urllib.parse.quote(parsed.query, safe="=&")
            encoded_url = urllib.parse.urlunsplit(
                (
                    parsed.scheme,
                    parsed.netloc,
                    encoded_path,
                    encoded_query,
                    parsed.fragment,
                )
            )
            response = requests.get(encoded_url, timeout=30)
        except Exception as e:
            print(f"Failed to download {url}: {e}")
            continue

        if response.status_code != 200:
            print(f"Failed to download {url}")
            continue

        output_path = os.path.join(args.output, id[:2], id + ".pdf")
        if not os.path.exists(os.path.dirname(output_path)):
            os.makedirs(os.path.dirname(output_path))

        with open(output_path, "wb") as f:
            f.write(response.content)

        n_bytes += response.content.__sizeof__()
        pbar.set_description(f"Downloaded {n_bytes / 1e6:.2f} MB")
