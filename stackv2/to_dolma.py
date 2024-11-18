import argparse
import json
import urllib
import datetime
import functools
import multiprocessing as mp
import polars as pl

from pathlib import Path
from licensed_pile.logs import configure_logging
from licensed_pile.write import to_dolma

SOURCE_NAME = "stackv2"
logger = configure_logging("stackv2")

# Load licenses only once at module level
with open(Path(__file__).parent / "blueoak.json") as f:
    BLUEOAK_LICENSES = {
        license["id"]: {"rating": rating["name"], "name": license["name"]}
        for rating in json.load(f)["ratings"]
        for license in rating["licenses"] if rating["name"] in ["Model", "Gold", "Silver", "Bronze"]
    }
BLUEOAK_KEYS = frozenset(BLUEOAK_LICENSES.keys())

def format_dolma(row):
    # Convert Polars row to dict for processing
    item = row
    
    metadata = {
        "license": ",".join(item["detected_licenses"]),
        "url": urllib.parse.urljoin(
            "https://raw.githubusercontent.com",
            "/".join([item['repo_name'], item['revision_id'], item['path']])
        )
    }
    
    # Add other metadata fields
    for key, value in item.items():
        if key != "content":
            metadata[key] = value.isoformat() if isinstance(value, datetime.datetime) else value

    return {
        "id": item["blob_id"],
        "text": item["content"],
        "source": SOURCE_NAME,
        "added": datetime.datetime.now(datetime.UTC).isoformat(),
        "created": item["revision_date"].isoformat(),
        "metadata": metadata
    }

def process_parquet(file_path, output_dir):
    parquet_stem = Path(file_path).stem
    logger.info(f"Parquet {parquet_stem}: started processing")
    
    # Read parquet file with Polars
    # Use streaming to handle large files efficiently
    df = pl.scan_parquet(file_path)
    
    # Filter for valid licenses
    # explode detected_licenses array and check if all values are in BLUEOAK_KEYS
    df_filtered = (
        df.with_columns(
            pl.col("detected_licenses").list.eval(pl.element().is_in(list(BLUEOAK_KEYS)))
            .alias("license_valid")
        )
        .filter(
            pl.col("license_valid").list.all()
        )
        .drop("license_valid")
    )
    
    def dolma_generator():
        # Process the filtered data in chunks
        for row in df_filtered.collect(streaming=True).iter_rows(named=True):
            yield format_dolma(row)
    
    dolma_name = f"stackv2-{parquet_stem}.jsonl.gz"
    to_dolma(dolma_generator(), output_dir, dolma_name, args.shard_size, quiet=True)
    
    logger.info(f"Parquet {parquet_stem}: finished processing")

def main(args):
    documents_dir = Path(args.output_dir) / "documents"
    documents_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = list(Path(args.data_path).rglob("*.parquet"))
    parquet_count = len(parquet_files)
    cpu_count = mp.cpu_count()
    np_proc = min(args.workers, parquet_count, cpu_count)
    
    # Use maxtasksperchild=1 to ensure clean worker state for each file
    with mp.Pool(np_proc, maxtasksperchild=1) as pool:
        pool.map(
            functools.partial(process_parquet, output_dir=documents_dir),
            parquet_files
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert HF data to dolma.")
    parser.add_argument(
        "--output_dir",
        default="data/stackv2",
        help="Where the dolma formatted data goes.",
    )
    parser.add_argument(
        "--data_path",
        help="Input folder with parquet files",
    )
    parser.add_argument(
        "--shard_size",
        type=int,
        default=4,
        help="Size, in GB, for each shard."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=4,
        help="Number of workers."
    )
    args = parser.parse_args()
    main(args)