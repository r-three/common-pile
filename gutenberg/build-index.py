"""Build index of PD books."""

import argparse
import glob
import json
import os
import urllib.parse
import tqdm
from rdflib import Graph


parser = argparse.ArgumentParser(description="Build an index of PD books.")
parser.add_argument("--data", default="data/cache/epub/**/*.rdf")
parser.add_argument("--format", default="xml")
parser.add_argument("--output", default="data/books.json")

QUERY = """
SELECT ?id ?title ?file
WHERE {
  ?p dcterms:rights ?rights
    FILTER regex(?rights, "^Public domain", "i")

  ?p dcterms:language ?l .
  ?l rdf:value "en"^^<http://purl.org/dc/terms/RFC4646> .
  ?l rdf:value ?lang .

  ?p dcterms:hasFormat ?file .
  ?file dcterms:format ?format_ .
  ?format_ rdf:value "text/plain"^^<http://purl.org/dc/terms/IMT> .

  ?p dcterms:title ?title
  BIND(?p as ?id)
}
"""


def parse_id(metadata):
    metadata["id"] = os.path.basename(urllib.parse.urlparse(metadata["id"]).path)
    return metadata


def main(args):
    results = []

    print("Parsing metadata")
    for i, filename in enumerate(tqdm.tqdm(glob.iglob(args.data))):
        g = Graph()
        g.parse(source=filename, format=args.format)
        results.extend(list(g.query(QUERY)))

    print(f"There are {len(results)} english public-domain books.")
    print(f"Writing index to {args.output}")

    results = map(lambda x: x.asdict(), results)
    results = map(parse_id, results)

    with open(args.output, "w") as wf:
        json.dump(list(results), wf)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
