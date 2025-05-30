import csv
import time

import requests

# Load the list of URLs
with open("libretext_content_urls.csv", "r") as f_in, open(
    "libretext_content.csv", "w"
) as f_out:
    writer = csv.writer(f_out)
    writer.writerow(["book_url", "section_name", "section_url", "section_content"])

    reader = csv.reader(f)
    headers = next(reader)

    for i, row in enumerate(reader):
        if i == 10:
            break
        book_url, section_name, section_url = row
        response = requests.get(section_url)
        section_content = response.text
        writer.writerow([book_url, section_name, section_url, section_content])
        time.sleep(1)
