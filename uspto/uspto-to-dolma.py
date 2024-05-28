import argparse
import multiprocessing
import re
import sys
from functools import partial
from itertools import islice
from pathlib import Path
from typing import Iterable, Iterator

import polars as pl
import pypandoc
from polars import col
from tqdm import tqdm

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.logs import configure_logging
from licensed_pile.write import to_dolma

logger = configure_logging("uspto")

if sys.version_info < (3, 9):
    raise RuntimeError("Python version >= 3.9 required")


def batched(iterable, n):
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def parse_html(html_string: str) -> str:
    if not html_string:
        return ""
    text = pypandoc.convert_text(html_string, "plain", "html", extra_args=["--quiet"])
    return re.sub(r"(?<!\n)\n(?!\n)", "", text)


# from: https://stackoverflow.com/a/74749075/19355181
def parallel_apply(max_concurrency: int, column: pl.Series) -> pl.Series:
    if max_concurrency == 0:
        max_concurrency = None
    with multiprocessing.get_context("spawn").Pool(4) as pool:
        return pl.Series(pool.imap(parse_html, column))


def process_datasets(
    data_dir: str = r"./data/uspto/",
    limit: int = 0,
    max_concurrency: int = 4,
) -> Iterable[dict]:
    """
    This function `run_dataset` scans a dataset located in a directory, converts each file in the dataset to a desired
    format using pandoc,and returns an iterable of dictionaries containing the converted data.

    Parameters:
    - `data_dir` (str): The directory where the dataset is located. Default value is "./data/uspto/".
    - `limit` (int): The maximum number of rows to convert. Default value is 0, which means convert all rows from all files in the dataset.
    - `max_concurrency` (int): The maximum number of concurrent conversions to perform. Default value is 2.

    Returns:
    - `Iterable[dict]`: An iterable of dictionaries containing the converted data from each file in the dataset.

    Note:
    - The `data_dir` parameter should be a valid directory path ending with a forward slash '/'.
    - The `limit` parameter determines how many row to read. Set it to 0 to convert all files.
    - The `max_concurrency` parameter determines how many parquet files to process concurrently.

    Example usage:
    ```python
    for data in run_dataset(data_dir=r"./data/uspto/", limit=10, max_concurrency=2):
        # Process each converted data entry
        print(data)
    ```
    """
    data_path = Path(data_dir)
    logger.info(f"Processing files in {data_path}")
    file_names = list(data_path.glob("*.parquet"))
    for file_name in file_names:
        yield from scan_dataset((file_name, limit, max_concurrency)).iter_rows(
            named=True
        )


def scan_dataset(args: tuple) -> pl.DataFrame:
    """
    Scans an individual parquet file and returns a processed DataFrame.

    Parameters:
        args (tuple): A tuple containing the file name, limit and max_concurrency.

    Returns:
        DataFrame: A processed DataFrame containing the selected columns from the dataset.

    Example Usage:
        file_name = "dataset.parquet"
        limit = 100
        max_concurrency = 4

        result = scan_dataset((file_name, limit, max_concurrency))
    """
    file_name, limit, max_concurrency = args
    parallel_apply_ = partial(parallel_apply, max_concurrency)
    columns = (
        "title_text",
        "title_language",
        "abstract_text",
        "description_html",
        "claims_html",
        "publication_date",
        "application_number",
        "filing_date",
    )

    df: pl.LazyFrame = (
        pl.scan_parquet(file_name)
        .select(columns)
        .filter(
            ~pl.all_horizontal(
                pl.col(["abstract_text", "description_html", "claims_html"]).is_null()
            )
        )
        # we use app no. for the id and filing date for the date created
        .rename({"application_number": "id", "filing_date": "created"})
        .with_columns(
            # the data was scrapped approx at this date
            pl.lit("2024-03-22", dtype=pl.String).alias("added"),
            col("created").cast(pl.String, strict=False),
            col("publication_date").cast(pl.String, strict=False),
            col("description_html")
            .map_batches(
                parallel_apply_,
                return_dtype=pl.String,
                is_elementwise=True,
            )
            .str.replace_all(r"\\left(\.|)|\\right(\.|)", ""),
            col("claims_html")
            .map_batches(
                parallel_apply_,
                return_dtype=pl.String,
                is_elementwise=True,
            )
            .str.replace_all(r"\\left(\.|)|\\right(\.|)", ""),
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
                pl.lit("\n\n", dtype=pl.String),
                col("description_html"),
                col("claims_html"),
                ignore_nulls=True,
            ).alias("text")
        )
    ).select(["id", "text", "added", "created", "title_language", "publication_date"])
    if limit > 0:
        df = df.fetch(limit).lazy()
    return df.collect(streaming=True)


def serialize_dolma(
    data_dir: str = r"./data/uspto/",
    limit: int = 0,
    max_concurrency: int = 4,
) -> Iterator[dict[str, str]]:
    """
    Serialize a dataset of documents into a standardized format.

    Args:
        data_dir: The directory path where the dataset files are located. Default is `./data/uspto/`.
        limit: The maximum number of documents to be serialized. Default is 0, which represents no limit.
        max_concurrency: max files to process in parallel. Default is `4`.

    Returns:
        A generator that yields dictionaries representing serialized documents. Each dictionary consists of the document
        content and metadata in a standardized format.

    Example Usage:
        for document in serialize_dolma(data_dir="./data/uspto/", limit=10):
            print(document)
    """
    for x in tqdm(process_datasets(data_dir, limit, max_concurrency)):
        metadata = {
            "source": "Google Patents Public Data",
            "metadata": {
                "license": str(PermissiveLicenses.CC_BY),
                "language": x.pop("title_language", "en"),
                "publication_date": str(x.pop("publication_date", "")),
            },
        }
        yield x | metadata


def create_args_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_dir", type=str, help="Output directory", default=r"raw"
    )
    parser.add_argument(
        "data_dir",
        type=str,
        default=r"/uspto/data/",
        help="Dataset directory where all parquet files to process are located ",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit the number of rows to read for testing",
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=8,
        help="Maximum number of parquet files to process concurrently",
    )
    return parser


if __name__ == "__main__":
    args = create_args_parser().parse_args()
    logger.info(
        f"""Processing USPTO with the following parameters: Output Dir: {args.output_dir},
         Limit: {args.limit}, Max Concurrency: {args.max_concurrency}"""
    )
    to_dolma(
        serialize_dolma(
            data_dir="/Users/baber/Downloads/untitled folder",
            limit=args.limit,
            max_concurrency=args.max_concurrency,
        ),
        args.output_dir,
        "uspto.jsonl.gz",
    )
