import argparse
import json
import urllib
import datetime
import functools
import multiprocessing.dummy as mp

from pathlib import Path
from datasets import load_dataset

from licensed_pile.logs import configure_logging
from licensed_pile.write import to_dolma
import multiprocessing as mp

SOURCE_NAME = "stackv2"

logger = configure_logging("stackv2")

BLUEOAK_LICENSES = {
    license["id"]: {"rating": rating["name"], "name": license["name"]}
    for rating in json.loads(open(Path(__file__).parent / "blueoak.json").read())["ratings"]
    for license in rating["licenses"] if rating["name"] in ["Model", "Gold", "Silver", "Bronze"]
}
BLUEOAK_KEYS = set(BLUEOAK_LICENSES.keys())

def format_dolma(item):
    dolma_dict = {
        "id": item["blob_id"],
        "text": item["content"],
        "source": SOURCE_NAME,
        "added": datetime.datetime.now(datetime.UTC).isoformat(),
        "created": item["revision_date"].isoformat(),
        "metadata": {
            "license": ",".join(item["detected_licenses"]),
            "url": urllib.parse.urljoin("https://raw.githubusercontent.com", "/".join([item['repo_name'], item['revision_id'], item['path']]))
        }
    }
    
    item.pop("content", None)
    
    for key, value in item.items():
        if isinstance(value, datetime.datetime):
            item[key] = value.isoformat()
    
    dolma_dict['metadata'].update(item)
    return dolma_dict

def process_parquet(file_path, output_dir):
    ds = load_dataset(
        "parquet", 
        data_files=str(file_path), 
        keep_in_memory=False, 
        split="train", 
        streaming=True
    )
    ds = ds.filter(lambda x: set(x["detected_licenses"]).issubset(BLUEOAK_KEYS))
    parquet_stem = file_path.stem
    
    dolma_name = f"stackv2-{parquet_stem}.jsonl.gz"
    
    def dolma_generator():
        for x in ds:
            yield format_dolma(x)
            
    logger.info(f"Parquet {parquet_stem}: started processing")
    to_dolma(dolma_generator(), output_dir, dolma_name, args.shard_size, quiet=True)
    logger.info(f"Parquet {parquet_stem}: finished processing")

def main(args):
    documents_dir = Path(args.output_dir) / "documents"
    documents_dir.mkdir(parents=True, exist_ok=True)

    parquet_files = list(Path(args.data_path).rglob("*.parquet"))
    
    parquet_count = len(list(parquet_files))
    cpu_count = mp.cpu_count()
    
    np_proc = min(args.workers, parquet_count)
    np_proc = min(np_proc, cpu_count)
    
    with mp.Pool(np_proc) as pool:
        pool.map(functools.partial(process_parquet, output_dir=documents_dir), parquet_files)

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
        "--shard_size", type=int, default=4, help="Size, in GB, for each shard."
    )
    parser.add_argument(
        "--workers", type=int, default=4, help="Number of workers."
    )
    args = parser.parse_args()
    main(args)
