import csv
import time

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager


def extract_books(driver, writer):
    """Extracts and stores books currently visible on the page."""
    global pbar
    try:
        # Get HTML elements in <article> tags with field data-cy="book-card"
        book_elements = driver.find_elements(
            By.XPATH, "//article[@data-cy='book-card']"
        )
        new_books = 0  # Counter for new links found

        for book in book_elements:
            # Write HTML to output file
            book_html = book.get_attribute("outerHTML")
            if book_html in all_books:
                continue
            writer.writerow([book.get_attribute("outerHTML")])
            all_books.add(book_html)
            new_books += 1

        if new_books > 0:
            pbar.update(new_books)  # Update tqdm progress bar with new books
    except StaleElementReferenceException:
        print("Warning: Stale elements encountered. Skipping this batch.")


def scroll_and_collect(driver, writer, pause_time=3):
    """Scrolls the page iteratively while collecting links at each step."""
    last_height = driver.execute_script("return document.body.scrollHeight")

    while True:
        extract_books(driver, writer)  # Collect books at current scroll position

        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);"
        )  # Scroll down
        time.sleep(pause_time)  # Wait for new content to load

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:  # Stop if no new content is loaded
            break
        last_height = new_height


# Set up WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in headless mode
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)

url = "https://pressbooks.directory/?__hstc=229531523.2d0cd9e59b7116c9bc48fe21fc321089.1742175509839.1742175509839.1742175509839.1&__hssc=229531523.1.1742175509839&__hsfp=2032059385&license=CC+BY%26%26Public+Domain%26%26CC+BY-SA%26%26CC0&per_page=50&lang=English%26%26English+(Canada)%26%26English+(United+States)%26%26English+(United+Kingdom)%26%26English+(Australia)"
min_words = 0
max_words = 10000
all_books = set()

with open("pressbooks_search_results.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(["book_card"])
    pbar = tqdm(total=0, unit=" links", desc="Scraping Links", dynamic_ncols=True)
    while min_words <= 70000:
        if min_words == 70000:
            max_words = None

        page_num = 1
        if max_words:
            curr_url = f"{url}&words=%3E={min_words}%26%26%3C={max_words}"
        else:
            curr_url = f"{url}&words=%3E={min_words}"

        driver.get(curr_url)
        time.sleep(3)
        try:
            results = driver.find_element(By.XPATH, "//h3[@id='results']")
        except:
            print("No results found.")
            continue

        num_results = int(results.text.split()[0].replace(",", ""))

        while page_num <= (num_results // 50) + 1:
            if max_words:
                curr_url = (
                    f"{url}&p={page_num}&words=%3E={min_words}%26%26%3C={max_words}"
                )
            else:
                curr_url = f"{url}&p={page_num}&words=%3E={min_words}"
            pbar.set_postfix(
                {
                    "Page": page_num,
                    "Min Words": min_words,
                    "Max Words": max_words,
                    "Num Results": num_results,
                }
            )

            driver.get(curr_url)
            scroll_and_collect(driver, writer)
            page_num += 1

        min_words += 10000
        max_words += 10000

    pbar.close()

# Close browser
driver.quit()
