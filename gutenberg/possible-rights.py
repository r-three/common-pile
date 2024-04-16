"""Find all the possible rights in PG metadata.

Output as of 2023-11-08
There are 2 kinds of rights to Gutenberg books.
	Copyrighted. Read the copyright notice inside this book for details.
	Public domain in the USA.
"""

import argparse
import glob

import tqdm
from rdflib import Graph

parser = argparse.ArgumentParser(description="Find all rights in the PG metadata.")
parser.add_argument("--data", default="data/cache/epub/**/*.rdf")
parser.add_argument("--format", default="xml")

QUERY = """
SELECT ?rights
WHERE {
  ?p dcterms:rights ?rights
}
"""


def main(args):
    rights = set()

    print("Parsing metadata")
    for i, filename in enumerate(tqdm.tqdm(glob.iglob(args.data))):
        g = Graph()
        g.parse(source=filename, format=args.format)
        rights.update([x["rights"].value for x in g.query(QUERY)])

    print(f"There are {len(rights)} kinds of rights to Gutenberg books.")
    for right in rights:
        print(f"\t{right}")


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
