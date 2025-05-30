import csv
import sys
from collections import defaultdict

from tqdm import tqdm

csv.field_size_limit(sys.maxsize)


books = defaultdict(lambda: defaultdict(list))
with open("pressbooks_content.csv", "r") as f_in, open(
    "pressbooks_content_no_duplicates.csv", "w"
) as f_out:
    reader = csv.reader(f_in)
    header = next(reader)

    writer = csv.writer(f_out)
    writer.writerow(header)

    for row in tqdm(list(reader), unit=" sections"):
        (
            title,
            author,
            subject,
            institution,
            language,
            license,
            last_updated,
            book_url,
            page_url,
            page_content,
        ) = row
        books[title][book_url].append(row)

    # Keep only the copy of each book with most total content
    for title, book_urls in tqdm(books.items(), unit=" books"):
        max_content = 0
        max_book_url = None
        for book_url, rows in book_urls.items():
            total_content = sum(len(row[-1]) for row in rows)
            if total_content > max_content:
                max_content = total_content
                max_book_url = book_url

        for row in books[title][max_book_url]:
            writer.writerow(row)
