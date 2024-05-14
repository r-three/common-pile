import argparse
import glob
from functools import partial
from typing import Iterable

import polars as pl
from download_preprocess import parse_html
from polars import col

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma


def scan_dataset(
    data_dir: str = r"./data/uspto/",
    url=r"http://localhost:3000/convert",
    limit: int = 0,
) -> Iterable[dict]:
    """
    Scans the dataset in the specified directory and yields dictionaries representing each row of data.
    The data is selected and transformed according to the specified columns.
    HTML content in the "description_html" and "claims_html" columns is parsed using the provided local URL.

    Parameters:
    - data_dir (str): The path to the directory containing the dataset. Defaults to "./data/uspto/".
    - url (str): The URL used for parsing HTML content. Defaults to "http://localhost:3000/convert".
    - streaming (bool): Do not load the whole dataset in Memory. Defaults to True.

    Returns:
    Iterable[dict]: An iterable of dictionaries representing each row of data.

    Example usage:
    ```python
    for row in scan_dataset(data_dir="./data/", url="http://example.com/"):
        print(row)
    ```
    """
    if not data_dir[-1] == r"/":
        data_dir += r"/"
    html_fn = partial(parse_html, url)

    # columns to use
    columns = [
        "title_text",
        "title_language",
        "abstract_text",
        "description_html",
        "claims_html",
        "publication_date",
        "application_number",
        "filing_date",
    ]
    for file_name in glob.glob(data_dir + r"*.parquet"):
        df: pl.LazyFrame = (
            pl.scan_parquet(file_name)
            .select(columns)
            .filter(
                ~pl.all_horizontal(
                    pl.col(
                        ["abstract_text", "description_html", "claims_html"]
                    ).is_null()
                )
            )
            # we use app no. for the id and filing date for the date added to database
            .rename({"application_number": "id", "filing_date": "added"})
            .with_columns(
                col("added").cast(pl.String, strict=False),
                col("publication_date").cast(pl.String, strict=False),
                col("description_html").map_elements(html_fn, return_dtype=pl.String),
                col("claims_html").map_elements(html_fn, return_dtype=pl.String),
                # if abstract returns `ABSTRACT\n\n<abstract>`. Null otherwise
                pl.concat_str(
                    pl.lit(r"ABSTRACT", dtype=pl.String),
                    pl.lit("\n\n", dtype=pl.String),
                    col("abstract_text"),
                    ignore_nulls=False,
                ).alias("abstract_text"),
            )
            .with_columns(
                pl.concat_str(
                    col("title_text"),
                    pl.lit("\n\n", dtype=pl.String),
                    col("abstract_text"),
                    col("description_html"),
                    col("claims_html"),
                    ignore_nulls=True,
                ).alias("text")
            )
        ).select(["id", "text", "added", "title_language", "publication_date"])
        if limit > 0:
            df = df.fetch(limit).lazy()
        yield from df.collect(streaming=True).iter_rows(named=True)


def serialize_dolma(ds: Iterable[dict[str, str]]) -> dict[str, str]:
    for x in ds:
        metadata = {
            "source": "Google Patents Public Data",
            "metadata": {
                "license": str(PermissiveLicenses.CC_BY),
                "language": x.pop("title_language", "en"),
                "publication_date": str(x.pop("publication_date", "9999")),
            },
        }
        yield x | metadata


parser = argparse.ArgumentParser()
parser.add_argument("--output_dir", type=str, help="Output directory", default=r"raw")
parser.add_argument("data_dir", type=str, help="Dataset directory")
parser.add_argument(
    "--url",
    type=str,
    help="REST API URL for the Node.js MathML to LaTeX converter",
    default=r"http://localhost:3000/convert",
)
parser.add_argument(
    "--limit", type=int, default=0, help="Limit the number of rows to read for testing"
)

if __name__ == "__main__":
    args = parser.parse_args()
    uspto_df = scan_dataset(
        args.data_dir,
        url=args.url,
        limit=args.limit,
    )
    to_dolma(serialize_dolma(uspto_df), args.output_dir, "uspto.jsonl.gz")
