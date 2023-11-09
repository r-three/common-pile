#!/usr/bin/env python3

import argparse
import json
import re
import tqdm
import glob
from google.cloud import storage
from rdflib import Graph

parser = argparse.ArgumentParser(
    description="Check if pg19 has any books that we don't."
)
parser.add_argument("--data", default="data/books.json")


def parse_id(name):
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


# 1546 Listed as Copyright but older than 1919 (in PG19)
# 3189 No plaintext version
# 378 Listed as Copyright but older than 1919 (in PG19)
# 38718 No plaintext version


def main(args):
    with open(args.data) as f:
        our_index = {x["id"] for x in json.load(f)}

    missing = {}
    client = storage.Client()

    for prefix in ("train", "validation", "test"):
        missing[prefix] = set()
        for blob in tqdm.tqdm(
            client.list_blobs("deepmind-gutenberg", prefix=f"{prefix}/")
        ):
            id = parse_id(blob.name)
            if id and id not in our_index:
                missing[prefix].add(id)

        print(
            f"pg19 has {len(missing[prefix])} books in the {prefix} split that we don't have..."
        )
        print(missing[prefix])

        for book in sorted(missing[prefix]):
            rdf_files = glob.glob(f"data/cache/epub/{book}/*.rdf")
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
