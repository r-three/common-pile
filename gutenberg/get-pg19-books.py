"""Download some books from the PG19 dataset."""

import argparse
import itertools
import json
import os
import urllib.request

import tqdm
from google.cloud import storage

from common_pile import logs

# These are books that are hard to get from PG, so we download them from pg19.
# See the README for more information on what these books are.
BOOKS = (28520, 30360, 57479, 57486, 38200, 3189, 26568, 51155, 38718)

parser = argparse.ArgumentParser(description="Download specific books from PG19")
parser.add_argument(
    "--books", nargs="+", default=BOOKS, help="Ids for books to download from PG19."
)
parser.add_argument(
    "--output_dir",
    default="data/raw_books/",
    help="Path to directory that contains the raw books.",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously downloaded copies of the book?",
)
parser.add_argument(
    "--index", default="data/books.json", help="Path to our index file."
)


# I was having issues getting the data via GCS, so we use the http version below.
def download_book(
    client, book: str, bucket="deepmind-gutenberg", prefix="train"
) -> str:
    blobs = list(client.list_blobs(bucket, prefix=f"{prefix}/{book}"))
    return blobs[0].download_as_text()


def download_book(_, book: str, bucket="deepmind-gutenberg", prefix="train") -> str:
    url = f"https://storage.googleapis.com/{bucket}/{prefix}/{book}.txt"
    data = urllib.request.urlopen(url)
    return data.read().decode("utf-8")


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)
    client = storage.Client()
    bucket = "deepmind-gutenberg"
    split = "train"
    to_add = []
    logger = logs.get_logger("gutenberg")
    for book in tqdm.tqdm(args.books):
        output_file = os.path.join(args.output_dir, f"{book}.txt")
        if os.path.exists(output_file) and not args.overwrite:
            continue
        logger.info(f"Downloading book {book} to {output_file}")
        with open(output_file, "w") as wf:
            try:
                wf.write(download_book(client, book) + "\n")
                to_add.append(
                    {
                        "id": book,
                        "file": f"https://storage.googleapis.com/{bucket}/{split}/{book}.txt",
                    }
                )
            except Exception as e:
                logger.error(e)
                os.remove(output_file)
                exit()

    # Add these books to our index so they are included going forward.
    with open(args.index) as f:
        og_index = json.load(f)
    og_length = len(og_index)
    og_index = {x["id"]: x for x in og_index}

    # If we have the metadata but no text (for example, they don't have a
    # plaintext link) add the pg19 link to the index.
    to_append = []
    for x in to_add:
        if x["id"] in og_index:
            logger.info(f"Updating file location for book {x['id']}")
            og_index[x["id"]]["file"] = x["file"]
        else:
            logger.info(f"Adding metadata for book {x['id']}")
            to_append.append(x)

    # TODO: Add safety with some sort of shadow page and file rename?
    with open(args.index, "w") as wf:
        # Add any extra ids to the index.
        json.dump(list(itertools.chain(og_index.values(), to_append)), wf)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("gutenberg")
    main(args)
