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
SELECT DISTINCT ?id ?title ?file ?format
WHERE {
  ?p dcterms:rights ?rights .
    FILTER regex(?rights, "^Public domain", "i") .

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


def parse_id(metadata):
    metadata["id"] = os.path.basename(urllib.parse.urlparse(metadata["id"]).path)
    return metadata


FILE_ORDERING = {
    "text/plain": 0,
    "text/plain; charset=utf-8": 1,
    "text/plain; charset=us-ascii": 2,
    "text/plain; charset=iso-8859-1": 3,
}


def file_type(results):
    if results:
        results = sorted(results, key=lambda x: FILE_ORDERING[str(x["format"])])
        return results[0:1]
    return results


def main(args):
    results = []

    print("Parsing metadata")
    for i, filename in enumerate(tqdm.tqdm(glob.iglob(args.data))):
        g = Graph()
        g.parse(source=filename, format=args.format)
        results.extend(file_type(g.query(QUERY)))

    print(f"There are {len(results)} english public-domain books.")
    print(f"Writing index to {args.output}")

    results = map(lambda x: x.asdict(), results)
    results = map(parse_id, results)

    with open(args.output, "w") as wf:
        json.dump(list(results), wf)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
