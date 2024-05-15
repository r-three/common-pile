import argparse
import glob
import multiprocessing
import sys
from functools import partial
from itertools import islice
from typing import Iterable, Iterator

import polars as pl
from download_preprocess import parse_html
from polars import col

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.logs import configure_logging, get_logger
from licensed_pile.write import to_dolma

logger = configure_logging()

if sys.version_info < (3, 9):
    raise RuntimeError("Python version >= 3.9 required")


def batched(iterable, n):
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def process_datasets(
    data_dir: str = r"./data/uspto/",
    url: str = r"http://localhost:3000/convert",
    limit: int = 0,
    max_concurrency: int = 4,
) -> Iterable[dict]:
    """
    This function `run_dataset` scans a dataset located in a directory, converts each file in the dataset to a desired
    format using an API endpoint,and returns an iterable of dictionaries containing the converted data.

    Parameters:
    - `data_dir` (str): The directory where the dataset is located. Default value is "./data/uspto/".
    - `url` (str): The API endpoint URL for converting the dataset files. Default value is "http://localhost:3000/convert".
    - `limit` (int): The maximum number of files to convert. Default value is 0, which means convert all files in the dataset.
    - `max_concurrency` (int): The maximum number of concurrent conversions to perform. Default value is 2.

    Returns:
    - `Iterable[dict]`: An iterable of dictionaries containing the converted data from each file in the dataset.

    Note:
    - The `data_dir` parameter should be a valid directory path ending with a forward slash '/'.
    - The `url` parameter should be a valid API endpoint URL.
    - The `limit` parameter determines how many files to convert. Set it to 0 to convert all files.
    - The `max_concurrency` parameter determines how many parquet files to process concurrently.

    Example usage:
    ```python
    for data in run_dataset(data_dir=r"./data/uspto/", url="http://localhost:3000/convert", limit=10, max_concurrency=2):
        # Process each converted data entry
        print(data)
    ```
    """
    if not data_dir[-1] == r"/":
        data_dir += r"/"

    # columns to use

    file_names = glob.glob(data_dir + r"*.parquet")
    if limit > 0:
        limit //= len(file_names)
        logger.info(f"Processing {limit} entries each from {len(file_names)} files.")
    args = [(x, url, limit) for x in file_names]
    with multiprocessing.get_context("spawn").Pool() as pool:
        for batch in batched(args, max_concurrency):
            logger.debug("Processing files %s", [b[0] for b in batch])
            for res in pool.imap_unordered(scan_dataset, batch):
                yield from res.iter_rows(named=True)


def scan_dataset(args: tuple) -> pl.DataFrame:
    """
    Scans a dataset and returns a processed DataFrame.

    Parameters:
        args (tuple): A tuple containing the file name, URL, and limit.

    Returns:
        DataFrame: A processed DataFrame containing the selected columns from the dataset.

    Example Usage:
        file_name = "dataset.parquet"
        url = "https://www.example.com/dataset"
        limit = 100

        result = scan_dataset((file_name, url, limit))
    """
    file_name, url, limit = args
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
    html_fn = partial(parse_html, url)
    df: pl.LazyFrame = (
        pl.scan_parquet(file_name)
        .select(columns)
        .filter(
            ~pl.all_horizontal(
                pl.col(["abstract_text", "description_html", "claims_html"]).is_null()
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
    return df.collect(streaming=True)


def serialize_dolma(
    data_dir: str = r"./data/uspto/",
    url=r"http://localhost:3000/convert",
    limit: int = 0,
    max_concurrency: int = 4,
) -> Iterator[dict[str, str]]:
    """
    Serialize a dataset of documents into a standardized format.

    Args:
        data_dir: The directory path where the dataset files are located. Default is `./data/uspto/`.
        url: The URL of the server to which the serialized documents will be sent. Default is `http://localhost:3000/convert`.
        limit: The maximum number of documents to be serialized. Default is 0, which represents no limit.
        max_concurrency: max files to process in parallel. Default is `4`.

    Returns:
        A generator that yields dictionaries representing serialized documents. Each dictionary consists of the document
        content and metadata in a standardized format.

    Example Usage:
        for document in serialize_dolma(data_dir="./data/uspto/", url="http://localhost:3000/convert", limit=10):
            print(document)
    """
    for x in process_datasets(data_dir, url, limit, max_concurrency):
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
        default=r"./data/uspto/",
        help="Dataset directory where all parquet files to process are located ",
    )
    parser.add_argument(
        "--url",
        type=str,
        help="REST API URL for the Node.js MathML to LaTeX converter",
        default=r"http://localhost:3000/convert",
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
        default=4,
        help="Maximum number of parquet files to process concurrently",
    )
    return parser


if __name__ == "__main__":
    args = create_args_parser().parse_args()
    logger.info(
        f"""Processing USPTO with the following parameters: Output Dir: {args.output_dir}, Data Dir: {args.data_dir},
        REST API URL: {args.url}, Limit: {args.limit}, Max Concurrency: {args.max_concurrency}"""
    )
    to_dolma(
        serialize_dolma(
            data_dir=args.data_dir,
            url=args.url,
            limit=args.limit,
            max_concurrency=args.max_concurrency,
        ),
        args.output_dir,
        "uspto.jsonl.gz",
    )
