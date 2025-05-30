import time

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

# Set up WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in headless mode
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)

# Open the LibreTexts catalog page
url = "https://commons.libretexts.org/catalog"
driver.get(url)

# Set to store unique links
all_links = set()

# Initialize tqdm progress bar
pbar = tqdm(total=0, unit=" links", desc="Scraping Links", dynamic_ncols=True)


def extract_links():
    """Extracts and stores links currently visible on the page."""
    global pbar
    try:
        link_elements = driver.find_elements(By.TAG_NAME, "a")
        new_links = 0  # Counter for new links found

        for link in link_elements:
            href = link.get_attribute("href")
            if href and href not in all_links:
                all_links.add(href)
                new_links += 1  # Increment new link count

        if new_links > 0:
            pbar.update(new_links)  # Update tqdm progress bar with new links
    except StaleElementReferenceException:
        print("Warning: Stale elements encountered. Skipping this batch.")


def scroll_and_collect(pause_time=3):
    """Scrolls the page iteratively while collecting links at each step."""
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        extract_links()  # Collect links at current scroll position

        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);"
        )  # Scroll down
        time.sleep(pause_time)  # Wait for new content to load

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:  # Stop if no new content is loaded
            break
        last_height = new_height


# Start scrolling and collecting links
scroll_and_collect()

# Close tqdm progress bar
pbar.close()

# Print final results
print(f"Total links collected: {len(all_links)}")

# Save to a file
with open("libretexts_book_urls.txt", "w") as f:
    for link in all_links:
        f.write(link + "\n")

# Close browser
driver.quit()
