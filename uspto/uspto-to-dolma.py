import argparse
import sys
from functools import partial
from pathlib import Path
from typing import Iterator

import polars as pl
from polars import col
from tqdm import tqdm
from utils import parallel_apply

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.logs import configure_logging
from licensed_pile.write import to_dolma

logger = configure_logging("uspto")

if sys.version_info < (3, 9):
    raise RuntimeError("Python version >= 3.9 required")


def process_datasets(
    data_dir: str = r"./data/uspto/",
    limit: int = 0,
    max_concurrency: int = 4,
) -> Iterator[dict]:
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
    for i, file_name in enumerate(file_names):
        for x in scan_dataset(file_name, limit, max_concurrency).iter_rows(named=True):
            yield x


def to_parquet(
    output_dir: str, data_dir: str, limit: int, max_concurrency: int
) -> None:
    output_dir = Path(output_dir)
    datapath = Path(data_dir)
    logger.info(
        f'Processing {len(list(datapath.glob("*.parquet")))} files in {datapath}'
    )
    for i, files in enumerate(tqdm(datapath.glob("*.parquet"))):
        file_path = output_dir.joinpath(f"uspto{i}")
        scan_dataset(files, limit, max_concurrency).write_parquet(file_path)


def scan_dataset(file_name, limit, max_concurrency) -> pl.DataFrame:
    """
    Scans an individual parquet file and returns a processed DataFrame.

    Returns:
        DataFrame: A processed DataFrame containing the selected columns from the dataset.

    Example Usage:
        file_name = "dataset.parquet"
        limit = 100
        max_concurrency = 4

        result = scan_dataset((file_name, limit, max_concurrency))
    """
    parallel_apply_desc = partial(parallel_apply, False, max_concurrency)
    parallel_apply_claims = partial(parallel_apply, True, max_concurrency)
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
            pl.concat_str(
                pl.lit(r"ABSTRACT", dtype=pl.String),
                pl.lit("\n\n", dtype=pl.String),
                col("abstract_text"),
                ignore_nulls=False,
            ).alias("abstract_text"),
        )
        .with_columns_seq(
            col("description_html")
            .map_batches(
                parallel_apply_desc,
                return_dtype=pl.String,
            )
            .str.replace_all(r"\\left(\.|)|\\right(\.|)", ""),
            col("claims_html")
            .map_batches(
                parallel_apply_claims,
                return_dtype=pl.String,
            )
            .str.replace_all(r"\\left(\.|)|\\right(\.|)", ""),
        )
        .with_columns(
            pl.concat_str(
                col("title_text"),
                pl.lit("\n\n", dtype=pl.String),
                col("abstract_text"),
                pl.lit("\n\n", dtype=pl.String),
                col("description_html"),
                pl.lit("\n\n", dtype=pl.String),
                col("claims_html"),
                ignore_nulls=True,
            ).alias("text"),
            pl.struct(
                pl.lit(str(PermissiveLicenses.CC_BY), dtype=pl.String).alias("license"),
                col("title_language").alias("language"),
                col("publication_date").alias("publication_date"),
            ).alias("metadata"),
            pl.lit("Google Patents Public Data").alias("source"),
        )
    ).select(["id", "text", "added", "created", "source", "metadata"])
    if limit > 0:
        df = df.fetch(limit).lazy()
    return df.collect(streaming=True)


def create_args_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output-path", type=str, help="Output directory", default=r"/uspto/outputs"
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default=r"/uspto/data",
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
    parser.add_argument(
        "--to-parquet",
        action="store_true",
        help="Output to parquet file",
    )

    return parser


if __name__ == "__main__":
    args = create_args_parser().parse_args()
    logger.info(
        f"""Processing USPTO with the following parameters: Output Dir: {args.output_path}, Data Dir: {args.data_path},
         Limit: {args.limit}, Max Concurrency: {args.max_concurrency}"""
    )
    if args.to_parquet:
        to_parquet(args.output_path, args.data_path, args.limit, args.max_concurrency)
    else:
        to_dolma(
            process_datasets(
                data_dir=args.data_path,
                limit=args.limit,
                max_concurrency=args.max_concurrency,
            ),
            args.output_path,
            "uspto.jsonl.gz",
        )
