import argparse
import requests
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import queue
import time

import jsonlines
from tqdm.auto import tqdm


logging.basicConfig(level=logging.INFO, format="download-files: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True, help="GovInfo API key")
    parser.add_argument("--links-file", required=True, help="Path to links file (jsonl)")
    parser.add_argument("--output-dir", required=True, help="Path to output directory")
    parser.add_argument("--workers", type=int, default=10, help="Number of threads")
    args = parser.parse_args()
    return args


def api_query(endpoint, headers, params):
    response = requests.get(endpoint, headers=headers, params=params)
    if response.status_code == 429:
        # Sleep for an hour if we've hit the rate-limit
        logging.info("Sleeping for one hour to avoid rate-limit")
        time.sleep(60*60)
        response = requests.get(endpoint, headers=headers, params=params)
    return response


def download_file(api_key, file):
    download_metadata = file["links"]
    link = download_metadata.get("txtLink")
    if link is not None:
        response = api_query(link, headers=None, params={"api_key": api_key})
        text = response.text
        return text
    return None


def construct_record(api_key, file):
    text = download_file(api_key, file)
    if text is None:
        return None
    return {
            "title": file["title"],
            "date": file["date"],
            "author": file["author"],
            "publisher": file["publisher"],
            "category": file["category"],
            "text": text
            }


def main(args):
    records = []
    with jsonlines.open(args.links_file, mode="r") as reader:
        with jsonlines.open(os.path.join(args.output_dir, "records.jsonl"), "w") as writer:
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                futures = [executor.submit(construct_record, args.api_key, file) for file in reader]
                for future in tqdm(as_completed(futures)):
                    record = future.result()
                    if record is not None:
                        writer.write(record)
                        


if __name__ == "__main__":
    args = parse_args()
    main(args)
