import os
import json
import wget
import requests
import argparse
import jsonlines
import multiprocessing as mp

from tqdm import tqdm
from pathlib import Path
from functools import partial
from datetime import datetime

import utils
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Download News Sites")
parser.add_argument(
    "--input_dir",
    default="data/news-propublica/",
    help="Path to output directory where raw pages are downloaded.",
)
parser.add_argument(
    "--output_dir",
    default="data/news-propublica/",
    help="Path to output directory for processed data.",
)
parser.add_argument(
    "--version",
    type=int,
    default=1,
    help="Version of the subset",
)
parser.add_argument(
    "--index_path",
    default=None,
    help="File that list of all pages",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously downloaded copies?",
)
parser.add_argument(
    "--filename", default="pro.jsonl.gz", help="The base filename for our data."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)
parser.add_argument(
    "--tag", type=str, default="div", help="Tag for the article or content"
)
parser.add_argument(
    "--attrs", type=json.loads, default=None, help="dict of attributes"
)
parser.add_argument(
    "--num_workers",
    default=None,
    help="Number of workers",
)

def get_record(page_index, input_dir=None, date=None, tag="div", attrs=None):
    idx = page_index["idx"]
    url = page_index["url"]
    filename = page_index["filename"]

    html_path = os.path.join(input_dir, filename)
    if os.path.exists(html_path):
        page_text = utils.get_text_from_page(html_path=html_path, tag=tag, attrs=attrs)

        return {
            "id": idx,
            "text": page_text,
            "source": url,
            "added": date,
            "metadata": {
                "license": "Creative Commons License (CC BY-NC-ND 3.0)",
            }
        }
    else:
        return None

def main(args):

    input_dir = os.path.join(args.input_dir)
    Path(input_dir).mkdir(parents=True, exist_ok=True)
    output_dir = os.path.join(args.output_dir, f"v{args.version}")
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    current_datetime = datetime.now()
    date = f"{current_datetime.year}-{current_datetime.month}-{current_datetime.day}"

    if args.index_path:
        with jsonlines.open(args.index_path) as reader:
            page_index = [line for line in reader]
    else:
        pagelist_path = os.path.join(input_dir, "pagelist.jsonl")
        if os.path.isfile(pagelist_path) and args.overwrite is False:
            with jsonlines.open(pagelist_path) as reader:
                page_index = [line for line in reader]
    
    # TODO Save HTML files
    # Then process/extract
    get_record_fn = partial(get_record, input_dir=input_dir, date=date, tag=args.tag, attrs=args.attrs)
    num_workers = mp.cpu_count() if args.num_workers is None else args.num_workers
    if num_workers == 1:
        page_data = list(map(get_record_fn, tqdm(page_index)))
    else:
        with mp.Pool(num_workers) as p:
            page_data = []
            for data in p.map(get_record_fn, tqdm(page_index)):
                page_data.append(data)

    page_data = [page for page in page_data if page is not None]

    # Cleaned Version
    output_dir = os.path.join(args.output_dir, f"v{args.version}")
    to_dolma(page_data, output_dir, args.filename, args.shard_size)

    return 0


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
