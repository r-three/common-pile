import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm.auto import tqdm
import csv
import re

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}

with open("oercommons_links.csv", "r") as f_in, open("oercommons_content.csv", "w") as f_out:
    reader = csv.reader(f_in)
    writer = csv.writer(f_out)

    header = next(reader)
    writer.writerow(["title", "search_result_link", "content_link", "content_html", "metadata_html"])

    links_collected = 0
    pbar = tqdm(reader)
    for row in pbar:
        try:
            title, search_result_link, content_link, metadata_html = row
            response = requests.get(content_link, headers=HEADERS)
            content_html = response.text
            writer.writerow([title, search_result_link, content_link, content_html, metadata_html])
            links_collected += 1
            pbar.set_postfix({"Links Collected": links_collected})
        except Exception as e:
            print(f"Skipping {content_link}. Error: {e}")
            continue
        time.sleep(0.2) 
