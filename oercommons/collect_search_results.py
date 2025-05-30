import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
from tqdm.auto import tqdm

BASE_URL = "https://oercommons.org/search?batch_size=100&sort_by=search&view_mode=list&f.license_types=no-strings-attached&f.license_types=public-domain&f.license_types=cc-by&f.license_types=cc-by-sa&source=courseware&f.language=en"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}
TOTAL_RESULTS = 8050  # Total number of results to scrape
BATCH_SIZE = 100  # Number of results per page

search_results = []

for batch_start in tqdm(range(0, TOTAL_RESULTS, BATCH_SIZE)):
    url = f"{BASE_URL}&batch_start={batch_start}"
    response = requests.get(url, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"Failed to fetch page {batch_start // BATCH_SIZE + 1}")
        continue
    
    soup = BeautifulSoup(response.text, "html.parser")
    results = soup.find_all("article", class_="js-index-item index-item clearfix")
    
    for result in results:
        title_tag = result.find("a", class_="item-link js-item-link")
        title = title_tag.get_text(strip=True) if title_tag else None
        link = title_tag["href"] if title_tag and title_tag.has_attr("href") else None
        if link:
            link = "https://oercommons.org" + link
        search_results.append({"Title": title, "Link": link, "html": str(result)})
    
    print(f"Fetched page {batch_start // BATCH_SIZE + 1}")
    time.sleep(5) # Respectful delay

# Convert to DataFrame
df = pd.DataFrame(search_results)

# Save results to CSV
df.to_csv("oercommons_results.csv", index=False)
print("Scraping complete. Data saved to oercommons_results.csv")
