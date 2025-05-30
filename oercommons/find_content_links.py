import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm.auto import tqdm
import csv
import re

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}

with open("oercommons_search_results.csv", "r") as f_in, open("oercommons_links.csv", "w") as f_out:
    reader = csv.reader(f_in)
    writer = csv.writer(f_out)

    header = next(reader)
    writer.writerow(["title", "search_result_link", "content_link", "metadata_html"])

    num_links = 0
    pbar = tqdm(reader)
    for row in pbar:
        try:
            search_result_link = row[1]
            if re.match(r"https://oercommons.org/courseware/lesson/*", search_result_link):
                response = requests.get(search_result_link, headers=HEADERS)
                soup = BeautifulSoup(response.text, "html.parser")
                content_link = soup.find_all("a", href=True, text=re.compile("View Resource"))[0]["href"]
                writer.writerow([row[0], row[1], content_link, row[2]])
                num_links += 1
                pbar.set_postfix({"num_links": num_links})
            elif re.match(r"https://oercommons.org/courseware/unit/*", search_result_link):
                response = requests.get(search_result_link, headers=HEADERS)
                soup = BeautifulSoup(response.text, "html.parser")
                all_links = soup.find_all("a", href=True)
                content_links = [l["href"] for l in all_links if re.match(r"https://oercommons.org/courseware/lesson/*", l["href"])]
                for content_link in content_links:
                    writer.writerow([row[0], row[1], content_link, row[2]])
                    num_links += 1
                    pbar.set_postfix({"num_links": num_links})
            else:
                print(f"Skipping {search_result_link}")
                continue
        except Exception as e:
            print(f"Skipping {search_result_link}. Error: {e}")
            continue

        time.sleep(0.2)  # Sleep to avoid overwhelming the server

