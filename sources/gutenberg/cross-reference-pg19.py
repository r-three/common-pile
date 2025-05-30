"""Check for books that are included in PG19 but not in our metadata dump."""

import argparse
import glob
import json
import re
from typing import Dict, Set

import tqdm
from google.cloud import storage
from rdflib import Graph

parser = argparse.ArgumentParser(
    description="Check if pg19 has any books that we don't."
)
parser.add_argument(
    "--data", default="data/books.json", help="The path to out index of books."
)


def parse_pg19_id(name):
    if m := re.match("^(?:train|validation|test)/(\d+).txt$", name):
        return m.groups(1)[0]
    return None


QUERY = """
SELECT DISTINCT ?title ?rights ?lang
WHERE {
  ?p dcterms:rights ?rights .

  ?p dcterms:language ?l .
  ?l rdf:value ?lang .

  ?p dcterms:title ?title
}
"""


def main(args):
    # All the books we have
    with open(args.data) as f:
        our_index: Set[str] = {x["id"] for x in json.load(f)}

    missing: Dict[str, Set[str]] = {}
    # Assumes you have default application credentials setup for access to
    # google cloud storage API.
    client = storage.Client()

    # For each split
    for prefix in ("train", "validation", "test"):
        missing[prefix] = set()
        # Iterate through all the blobs they have in the bucket.
        for blob in tqdm.tqdm(
            client.list_blobs("deepmind-gutenberg", prefix=f"{prefix}/")
        ):
            book_id = parse_pg19_id(blob.name)
            if book_id and book_id not in our_index:
                missing[prefix].add(book_id)

        print(
            f"pg19 has {len(missing[prefix])} books in the {prefix} split that we don't have..."
        )
        print(missing[prefix])

        # Find metadata about each of the missing books
        for book in sorted(missing[prefix]):
            print("# " + "-" * 80)
            # Find the rdf file with the metadata.
            rdf_files = glob.glob(f"data/cache/epub/{book}/*.rdf")
            # Some books seem to have been removed from Project Gutenberg.
            if not rdf_files:
                print(f"Book {book} is missing from PG.")
                continue
            print(f"Information about book {book}")
            for f in rdf_files:
                g = Graph()
                g.parse(source=f, format="xml")
                results = list(g.query(QUERY))
                for result in results:
                    print(f"\tTitle: {result['title']}")
                    print(f"\tRights: {result['rights']}")
                    print(f"\tLanguage: {result['lang']}")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
