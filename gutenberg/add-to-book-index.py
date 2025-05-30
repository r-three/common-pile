#!/usr/bin/env python3

import argparse
import itertools
import json
import os
import urllib.parse

from rdflib import Graph
from utils import file_type, parse_id

from common_pile import logs

# These books are in PG, but their metadata is messed up. We force their addition
# to the index here.
BOOKS = (1546, 378)

parser = argparse.ArgumentParser(description="Add specific books to the main index.")
parser.add_argument(
    "--books", default=BOOKS, nargs="+", help="Ids of the books to add to the index."
)
parser.add_argument(
    "--data", default="data/cache/epub", help="Path to the directory of rdf metadata."
)
parser.add_argument(
    "--index", default="data/books.json", help="Path to the book index we are updating."
)
parser.add_argument("--format", default="xml", help="The format of the rdf metadata.")

QUERY = """
SELECT DISTINCT ?id ?title ?file ?format ?lang
WHERE {
  ?p dcterms:language ?l .
  ?l rdf:value ?lang .

  ?p dcterms:hasFormat ?file .
  ?file dcterms:format ?format_ .
    FILTER (!regex(str(?file), ".zip$", "i")) .
    FILTER (!regex(str(?file), "README", "i")) .

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
    logger = logs.get_logger("gutenberg")
    logger.info("Parsing metadata.")
    results = []
    for book in args.books:
        g = Graph()
        g.parse(
            source=os.path.join(args.data, book, f"pg{book}.rdf"), format=args.format
        )
        results.extend(file_type(g.query(QUERY)))

    results = map(lambda x: x.asdict(), results)
    results = list(map(parse_id, results))
    logger.info(f"Built {len(results)} extra metadata entries.")

    logger.info("Adding new metadata to the index.")
    with open(args.index) as f:
        og_results = json.load(f)

    # TODO: Add safety with some sort of shadow page and file rename?
    with open(args.index, "w") as wf:
        json.dump(list(itertools.chain(og_results, results)), wf)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("gutenberg")
    main(args)
