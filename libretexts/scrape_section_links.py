import time
import csv

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from tqdm import tqdm


def click_show_buttons():
    """Clicks the 'Show' buttons next to 'Licensing' to reveal the Breakdown section."""
    pbar = tqdm(desc="Clicking 'Show' Buttons", unit=" buttons", leave=False)
    while True:
        try:
            show_button = WebDriverWait(driver, 2).until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Show')]"))
            )
            show_button.click()
            pbar.update(1)
            time.sleep(1)  # Allow time for content to load
        except (NoSuchElementException, ElementClickInterceptedException, TimeoutException):
            break


def scroll_to_bottom():
    """Scrolls down incrementally to ensure all content loads."""
    last_height = driver.execute_script("return document.body.scrollHeight")

    pbar = tqdm(desc="Scrolling to Bottom", unit=" pixels", leave=False) 
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")  # Scroll down
        time.sleep(2)  # Allow time for content to load

        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:  # Stop if no new content is loading
            break
        last_height = new_height
        pbar.update(new_height - last_height)


def get_section_carets():
    """Finds all caret buttons next to the section links."""
    try:
        # Locate all carets next to section links
        carets = driver.find_elements(By.XPATH, "//i[contains(@class, 'caret right icon cursor-pointer')]")

        # Filter to only carets that are not Front Matter or Back Matter
        carets = [caret for caret in carets if caret.find_element(By.XPATH, "./following-sibling::div[@class='content']//a").text.strip() not in ["Front Matter", "Back Matter"]]
        return carets
    except NoSuchElementException:
        print("Could not find any section carets.")
        return []


def click_all_carets(carets):
    """Clicks each caret button to expand its section and reveal subsections."""
    for caret in tqdm(carets, desc="Expanding Sections", unit=" sections", leave=False):
        try:
            section_title_element = caret.find_element(By.XPATH, "./following-sibling::div[@class='content']//a")
            section_title = section_title_element.text.strip()

            # Skip clicking for "Front Matter" and "Back Matter"
            if section_title in ["Front Matter", "Back Matter"]:
                continue
            
            driver.execute_script("arguments[0].click();", caret)  # Click via JavaScript
            time.sleep(1)  # Allow time for content to expand
        except Exception as e:
            print(f"Error clicking caret: {e}")


def get_all_subsection_links():
    """Finds only subsection links, ignoring section links."""
    subsections = {}

    try:
        # Find all subsection items INSIDE a section's <div class="list">
        #subsection_items = driver.find_elements(By.XPATH, "//div[@class='list']//div[@class='item' and @role='listitem']")
        #subsection_items = driver.find_elements(By.XPATH, "//div[@class='list']//div[@class='item' and @role='listitem' and not(./div[@class='list'])]")
        subsection_items = driver.find_elements(By.XPATH, "//div[@class='item' and @role='listitem'][.//i[contains(@class, 'circle tiny icon')]]")
        
        for item in tqdm(subsection_items, desc="Extracting Subsections", unit=" subsections", leave=False):
            try:
                # Find the <a> tag inside the subsection item
                link_element = item.find_element(By.XPATH, ".//a")

                # Extract subsection title and URL
                title = link_element.text.strip()
                url = link_element.get_attribute("href")

                if title and url:
                    subsections[title] = url

            except NoSuchElementException:
                print("Warning: Found a subsection item without a valid link.")

    except NoSuchElementException:
        print("No subsection links found.")

    return subsections


# Set up Selenium WebDriver
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in headless mode
options.add_argument("--disable-gpu")
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Example LibreTexts book link
with open("libretext_book_urls.txt", "r") as file:
    book_urls = file.readlines()

with open("libretext_content_urls.csv", "w") as f:
    writer = csv.writer(f)
    writer.writerow(["book_url", "section_name", "section_url"])

    num_links = 0
    pbar = tqdm(book_urls, desc="Processing Books", unit=" books")
    for book_url in pbar:
        driver.get(book_url)

        # Step 1: Click all "Show" buttons to reveal hidden content
        click_show_buttons()
        while True:
            # Step 2: Get all the caret buttons to expand sections
            carets = get_section_carets()
            # Step 3: If no carets are found, break out of the loop
            if len(carets) == 0:
                break
            # Step 4: Click each caret to reveal subsection links
            click_all_carets(carets)


        # Step 3: Extract all subsection links
        subsections = get_all_subsection_links()

        # Step 4: Write to CSV
        writer.writerows([[book_url.strip(), section_name, section_url.strip()] for section_name, section_url in subsections.items()])
        
        num_links += len(subsections)
        pbar.set_postfix({"Links Found": num_links})

# Close browser
driver.quit()
