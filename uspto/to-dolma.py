import argparse
import datetime
import os

import datasets
from bs4 import BeautifulSoup

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma


def format_text(example: dict) -> dict:
    return {"text": example["text"]}


def format_equation(equation: str) -> str:
    return equation


def format_description(desc: str) -> str:
    return desc.strip()


def return_dolma(ds: datasets.Dataset) -> dict:
    for x in ds:
        return x


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
