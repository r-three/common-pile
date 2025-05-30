import csv
import re
import sys
from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

csv.field_size_limit(sys.maxsize)


with open("pressbooks_links.csv", "r") as f_in, open(
    "pressbooks_content.csv", "w"
) as f_out:
    reader = csv.reader(f_in)
    header = next(reader)

    writer = csv.writer(f_out)
    writer.writerow(
        [
            "title",
            "author",
            "subject",
            "institution",
            "language",
            "license",
            "last_updated",
            "book_url",
            "page_url",
            "page_content",
        ]
    )

    num_sections = 0
    pbar = tqdm(list(reader), unit=" books")
    for row in pbar:
        (
            title,
            author,
            subject,
            institution,
            language,
            license,
            last_updated,
            book_url,
        ) = row
        try:
            response = requests.get(book_url, timeout=10)
        except Exception as e:
            print(f"Error getting {book_url}: {e}")
            continue

        if response.status_code != 200:
            print(f"Error getting {book_url}: {response.status_code}")
            continue

        book_url_parsed = urlparse(book_url)
        base_url = f"{book_url_parsed.scheme}://{book_url_parsed.netloc}"
        path_parts = book_url_parsed.path.strip("/").split("/")
        book_name = path_parts[0] if path_parts else ""

        soup = BeautifulSoup(response.text, "html.parser")
        li_tags = soup.find_all("li", id=re.compile("toc-chapter*"))
        links = []
        for li_tag in li_tags:
            links.extend([link["href"] for link in li_tag.find_all("a", href=True)])

        if len(links) == 0:
            print(f"No links found for {book_url}")
            continue

        for link in tqdm(links, unit=" sections", leave=False):
            try:
                response = requests.get(link, timeout=10)
            except Exception as e:
                print(f"Error getting {link}: {e}")
                continue

            if response.status_code != 200:
                print(f"Error getting {link}: {response.status_code}")
                continue

            content = response.text
            writer.writerow(
                [
                    title,
                    author,
                    subject,
                    institution,
                    language,
                    license,
                    last_updated,
                    book_url,
                    link,
                    content,
                ]
            )
            num_sections += 1
            pbar.set_description(f"Processed {num_sections} sections")
