import sys
import csv
import requests

from bs4 import BeautifulSoup
from tqdm import tqdm


csv.field_size_limit(sys.maxsize)


with open("pressbooks_search_results.csv", "r") as f_in, open("pressbooks_links.csv", "w") as f_out:
    writer = csv.writer(f_out)
    writer.writerow(["title", "author", "subject", "institution", "language", "license", "last_updated", "book_url"])

    reader = csv.reader(f_in)
    header = next(reader)
    for i, row in enumerate(tqdm(reader)):
        if i < 1785:
            continue
        html = row[0]
        soup = BeautifulSoup(html, "html.parser")

        link = soup.find("a", {"class": "text-red-700 underline hover:text-red-900"})
        if link is None:
            print(f"Failed to find link on page for row {i}")
            continue
        title = link.text.strip()
        url = link["href"]
        
        author_tag = soup.find("p", {"data-cy": "book-authors"})
        author = "" if author_tag is None else author_tag.find("span").text.strip()

        subject_tag = soup.find("p", {"data-cy": "book-subjects"})
        subject = "" if subject_tag is None else subject_tag.find("span").text.strip()

        last_updated_tag = soup.find("p", {"data-cy": "book-last-updated"})
        last_updated = "" if last_updated_tag is None else last_updated_tag.find("span").text.strip()

        institution_tag = soup.find("p", {"data-cy": "book-institutions"})
        institution = "" if institution_tag is None else institution_tag.find("span").text.strip()

        language_tag = soup.find("p", {"data-cy": "book-language"})
        language = "" if language_tag is None else language_tag.find("span").text.strip()

        license_tag = soup.find("span", {"data-cy": "book-copyright-license"})
        if license_tag is None:
            print(f"Failed to find license tag for row {i}")
            continue
        license = "" if license_tag is None else license_tag.text.strip()
        
        try:
            response = requests.get(url, timeout=10)
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
            continue
        if response.status_code == 200:
            response_html = response.text
            response_soup = BeautifulSoup(response_html, "html.parser")
            book_url_tag = response_soup.find("a", class_="call-to-action", href=True)
            if book_url_tag:
                book_url = book_url_tag["href"]
                writer.writerow([title, author, subject, institution, language, license, last_updated, book_url])
            else:
                print(f"Failed to find book url tag for {url}")
                continue
        else:
            print(f"Failed to fetch {url}: {response.status_code}")
            continue
