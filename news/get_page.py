import os
import wget
import requests
import argparse
import jsonlines
import multiprocessing as mp

from tqdm import tqdm
from pathlib import Path
from functools import partial

import utils

parser = argparse.ArgumentParser(description="Download News Sites")
parser.add_argument(
    "--url", default="https://www.propublica.org/", help="Base URL"
)
parser.add_argument(
    "--output_dir",
    default="data/news-propublica/",
    help="Path to output directory where raw pages are downloaded.",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously downloaded copies?",
)
parser.add_argument(
    "--num_workers",
    default=None,
    help="Number of workers",
)

def get_pages(page_index, output_path):
    idx = page_index["idx"]
    url = page_index["url"]
    page_file_path = os.path.join(output_path, f"{idx}.html")
    try:
        wget.download(url, out=page_file_path)
        return 0
    except:
        try:
            page = requests.get(url)  
            with open(page_file_path, 'wb') as fp:
                fp.write(page.content)
            return 0
        except:
            return url

def main(args):

    raw_output_dir = os.path.join(args.output_dir, "raw")
    Path(raw_output_dir).mkdir(parents=True, exist_ok=True)
    cleaned_output_dir = os.path.join(args.output_dir, f"v{args.version}")
    Path(cleaned_output_dir).mkdir(parents=True, exist_ok=True)

    if args.index_path:
        with jsonlines.open(args.index_path) as reader:
            page_index = [line for line in reader]
    else:
        pagelist_path = os.path.join(raw_output_dir, "pagelist.jsonl")
        if os.path.isfile(pagelist_path) and args.overwrite is False:
            with jsonlines.open(pagelist_path) as reader:
                page_index = [line for line in reader]
        else:
            page_list = utils.build_url_index(args.url)
            page_index = [{"idx": idx, "url": url} for idx, url in enumerate(page_list)]
            with jsonlines.open(pagelist_path, mode="w") as writer:
                writer.write_all(page_index)
    
    # Download all pages
    download_fn = partial(get_pages, output_path=raw_output_dir)
    num_workers = mp.cpu_count() if args.num_workers is None else args.num_workers
    if num_workers == 1:
        failed_pages = list(map(download_fn, tqdm(page_index)))
    else:
        with mp.Pool(num_workers) as p:
            failed_pages = list(p.map(download_fn, tqdm(page_index)))

    failedlist_path = os.path.join(raw_output_dir, "failedlist.jsonl")
    with jsonlines.open(failedlist_path, mode="w") as writer:
        writer.write_all([{"idx": idx, "url": pages} for idx, pages in enumerate(failed_pages)])

    return 0


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)