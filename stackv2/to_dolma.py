import argparse
import json
import urllib
import datetime

from pathlib import Path
from datasets import load_dataset

from licensed_pile.logs import configure_logging
from licensed_pile.write import to_dolma

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
            "license": item["detected_licenses"],
            "url": urllib.parse.urljoin("https://raw.githubusercontent.com", "/".join([item['repo_name'], item['revision_id'], item['path']]))
        }
    }
    
    item.pop("content", None)
    
    for key, value in item.items():
        if isinstance(value, datetime.datetime):
            item[key] = value.isoformat()
    
    dolma_dict['metadata'].update(item)
    return dolma_dict

def main(args):
    documents_dir = Path(args.output_dir) / "documents"
    documents_dir.mkdir(parents=True, exist_ok=True)
    
    ds = load_dataset(args.data_path, keep_in_memory=False, split="train", streaming=True)
    ds = ds.filter(lambda x: set(x["detected_licenses"]).issubset(BLUEOAK_KEYS))
    
    def dolma_generator():
        for x in ds:
            yield format_dolma(x)
    
    to_dolma(dolma_generator(), documents_dir, "stackv2.jsonl.gz", args.shard_size)
    logger.info(f"Saved {args.data_path} as dolma sharded files at {args.output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert HF data to dolma.")
    parser.add_argument(
        "--output_dir",
        default="data/stackv2",
        help="Where the dolma formatted data goes.",
    )
    parser.add_argument(
        "--data_path",
        help="Input folder or HF dataset path",
    )
    parser.add_argument(
        "--shard_size", type=int, default=4, help="Size, in GB, for each shard."
    )
    args = parser.parse_args()
    main(args)
