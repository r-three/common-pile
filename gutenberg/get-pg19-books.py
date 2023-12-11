"""Download some books from the PG19 dataset."""

import argparse
import itertools
import os
import tqdm
from google.cloud import storage

parser = argparse.ArgumentParser(description="Download specific books from PG19")
parser.add_argument("books", nargs="+", help="Ids for books to download from PG19.")
parser.add_argument("--output_dir", default="data/raw_books/", help="Path to directory that contains the raw books.")
parser.add_argument("--overwrite", action="store_true", help="Should we overwrite previously downloaded copies of the book?")
parser.add_argument("--index", default="data/books.json", help="Path to our index file.")


def download_book(
    client, book: str, bucket="deepmind-gutenberg", prefix="train"
) -> str:
    blobs = list(client.list_blobs(bucket, prefix=f"{prefix}/{book}"))
    return blobs[0].download_as_text()


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)
    client = storage.Client()
    to_add = []
    for book in tqdm.tqdm(args.books):
        output_file = os.path.join(args.output_dir, f"{book}.txt")
        if os.path.exists(output_file) and not args.overwrite:
            continue
        with open(output_file, "w") as wf:
            wf.write(download_book(client, book) + "\n")
        to_add.append({"id": book, "file": f"gs://{bucket}/train/{book}.txt"})

    # Add these books to our index so they are included going forward.
    with open(args.index) as f:
        og_index = json.load(f)
    og_length = len(og_index)
    og_index = {x['id']: x for x in og_index}
    assert len(og_index) == og_length

    # If we have the metadata but no text (for example, they don't have a
    # plaintext link) add the pg19 link to the index.
    to_append = []
    for x in to_add:
        if x['id'] in og_index:
            og_index[x['id']]['file'] = x['id']
        else:
            to_append.append(x)

    # TODO: Add safety with some sort of shadow page and file rename?
    with open(args.index, "w") as wf:
        # Add any extra ids to the index.
        json.dump(list(itertools.chain(og_index, to_append)), wf)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
