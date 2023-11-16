"""Download some books from the PG19 dataset."""

import argparse
import os
import tqdm
from google.cloud import storage

parser = argparse.ArgumentParser(description="Download specific books from PG19")
parser.add_argument("books", nargs="+")
parser.add_argument("--output_dir", default="data/books/")
parser.add_argument("--overwrite", action="store_true")


def download_book(
    client, book: str, bucket="deepmind-gutenberg", prefix="train"
) -> str:
    blobs = list(client.list_blobs(bucket, prefix=f"{prefix}/{book}"))
    return blobs[0].download_as_text()


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)
    client = storage.Client()
    for book in tqdm.tqdm(args.books):
        output_file = os.path.join(args.output_dir, f"{book}.txt")
        if os.path.exists(output_file) and not args.overwrite:
            continue
        with open(output_file, "w") as wf:
            wf.write(download_book(client, book) + "\n")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
