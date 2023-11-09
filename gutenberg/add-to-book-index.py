#!/usr/bin/env python3

import argparse
import itertools
import json
import os
import urllib.parse
from rdflib import Graph
from utils import parse_id, file_type


parser = argparse.ArgumentParser(description="Add specific books to the main index.")
parser.add_argument("books", nargs="+")
parser.add_argument("--data", default="data/cache/epub")
parser.add_argument("--index", default="data/books.json")

QUERY = """
SELECT DISTINCT ?id ?title ?file ?format
WHERE {
  ?p dcterms:language ?l .
  ?l rdf:value "en"^^<http://purl.org/dc/terms/RFC4646> .
  ?l rdf:value ?lang .

  ?p dcterms:hasFormat ?file .
  ?file dcterms:format ?format_ .
    FILTER (!regex(str(?file), ".zip$", "i")) .

  {?format_ rdf:value "text/plain"^^<http://purl.org/dc/terms/IMT> }
  UNION
  {?format_ rdf:value "text/plain; charset=utf-8"^^<http://purl.org/dc/terms/IMT> }
  UNION
  {?format_ rdf:value "text/plain; charset=us-ascii"^^<http://purl.org/dc/terms/IMT> }
  UNION
  {?format_ rdf:value "text/plain; charset=iso-8859-1"^^<http://purl.org/dc/terms/IMT> } .

  ?format_ rdf:value ?format .
  ?p dcterms:title ?title
  BIND(?p as ?id)
}
"""


def main(args):
    print("Parsing metadata.")
    results = []
    for book in args.books:
        g = Graph()
        g.parse(source=os.path.join(args.data, book, f"pg{book}.rdf"), format="xml")
        results.extend(file_type(g.query(QUERY)))

    results = map(lambda x: x.asdict(), results)
    results = list(map(parse_id, results))
    print(f"Built {len(results)} extra metadata entries.")

    print("Adding new metadata to the index.")
    with open(args.index) as f:
        og_results = json.load(f)

    with open(args.index, "w") as wf:
        json.dump(list(itertools.chain(og_results, results)), wf)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
