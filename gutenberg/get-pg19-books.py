"""Download some books from the PG19 dataset."""

import argparse
import os
from google.cloud import storage

parser = argparse.ArgumentParser(description="Download specific books from PG19")
parser.add_argument("books", nargs="+")
parser.add_argument("--output_dir", default="data/books/")


def download_book(
    client, book: str, bucket="deepmind-gutenberg", prefix="train"
) -> str:
    blobs = list(client.list_blobs(bucket, prefix=f"{prefix}/{book}"))
    return blobs[0].download_as_text()


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)
    client = storage.Client()
    for book in args.books:
        with open(os.path.join(args.output_dir, f"{book}.txt"), "w") as wf:
            wf.write(download_book(client, book) + "\n")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
