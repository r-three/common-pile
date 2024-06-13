"""Build index of PD books."""

import argparse
import glob
import json
import os
import urllib.parse

import tqdm
from rdflib import Graph
from utils import file_type, parse_id

from licensed_pile import logs

# These books are not good data for Language Modeling, they are boilerplate
# descriptions for data formats for recorded music and how PG books were
# distributed on disk. We skip adding these to the index these.
SKIP = (5627, 5634, 5635, 4949, 4950, 4951, 4749, 4750, 4751, 10802, 11220)


parser = argparse.ArgumentParser(description="Build an index of PD books.")
parser.add_argument(
    "--data",
    default="data/cache/epub/**/*.rdf",
    help="Glob pattern that matches all metadata files.",
)
parser.add_argument("--format", default="xml", help="The format of the rdf metadata.")
parser.add_argument(
    "--output", default="data/books.json", help="Path to save the book index to."
)
parser.add_argument(
    "--skip", default=SKIP, nargs="+", help="Known bad book ids to skip."
)

# Add this line to the language part of the query to filter to English only.
# ?l rdf:value "en"^^<http://purl.org/dc/terms/RFC4646> .

# TODO Update query to prefer zip files for faster dl?
QUERY = """
SELECT DISTINCT ?id ?title ?file ?format ?lang
WHERE {
  ?p dcterms:rights ?rights .
    FILTER regex(?rights, "^Public domain", "i") .

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
    results = []
    skip = set(args.skip)

    logger = logs.get_logger("gutenberg")
    logger.info("Parsing metadata")
    for i, filename in enumerate(tqdm.tqdm(glob.iglob(args.data))):
        id = os.path.basename(os.path.dirname(filename))
        if id in skip:
            continue
        g = Graph()
        g.parse(source=filename, format=args.format)
        results.extend(file_type(g.query(QUERY)))

    logger.info(f"There are {len(results)} public-domain books.")
    logger.info(f"Writing index to {args.output}")

    results = map(lambda x: x.asdict(), results)
    results = map(parse_id, results)

    with open(args.output, "w") as wf:
        json.dump(list(results), wf)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("gutenberg")
    main(args)
