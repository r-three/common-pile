import datetime

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
    for x in ds.map(format_text, remove_columns=ds.column_names).to_dict():
        return x


if __name__ == "__main__":
    YEAR = 1975
    DATASET = "baber/USPTO"
    uspto_df = datasets.load_dataset(DATASET, split="train", streaming=True)
    uspto_df = uspto_df.filter(lambda x: x["publication_date"].year < YEAR)
    uspto_df = uspto_df.map(format_text, remove_columns=uspto_df.column_names)
