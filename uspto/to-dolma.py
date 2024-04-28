import argparse
import datetime
import os

import datasets
from bs4 import BeautifulSoup

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma


def format_text(example: dict) -> dict:
    """
    Title\n\n
    abstract_text\n\n
    description\n\n
    claims
    """
    output = ""
    if title := example.get("title_text"):
        output += title + "\n\n"
    if abstract := example.get("abstract_text"):
        output += BeautifulSoup(abstract, "html.parser").get_text().strip() + "\n\n"
    if description := example.get("description_html"):
        description = BeautifulSoup(description, "html.parser")
        equations: list = description.find_all("maths")
        if equations:
            for i, eq in enumerate(equations):
                eq.string = format_equation(eq)
        output += description.get_text().strip() + "\n\n"
    if claims := example.get("claims_text"):
        claims = BeautifulSoup(claims, "html.parser")
        equations: list = claims.find_all("maths")
        if equations:
            for i, eq in enumerate(equations):
                eq.string = format_equation(eq)
        output += claims.get_text().strip()

    return {
        "text": output,
        "date": example.get("publication_date"),
    }


def format_equation(equation: str) -> str:
    # TODO: parse mathml to TEX
    return equation


def return_dolma(ds: datasets.Dataset) -> dict:
    for x in ds:
        try:
            output = {
                "text": x.get("text"),
                "added": datetime.datetime.now(),
                "id": x.get("publication_number"),
                "source": "USPTO",
                "metadata": {
                    "license": str(PermissiveLicenses.PD),
                    "language": x.get("title_language"),
                    "year": x.get("publication_date").year,
                },
            }
        except:
            output = None
        yield output


def parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", type=str, help="Output directory")
    parser.add_argument("--year", type=int, help="minimum year to filter", default=1975)
    return parser


if __name__ == "__main__":
    args = parser().parse_args()
    YEAR = 1975
    DATASET = "baber/USPTO"
    uspto_df = datasets.load_dataset(DATASET, split="train", streaming=True)
    uspto_df = uspto_df.filter(lambda x: x["publication_date"].year < YEAR)
    uspto_df = uspto_df.map(format_text, remove_columns=uspto_df.column_names)
    os.makedirs(args.output_dir, exist_ok=True)
    to_dolma(return_dolma(uspto_df), args.output_dir, "uspto.jsonl.gz")
