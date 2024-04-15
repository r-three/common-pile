import argparse
import requests
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import time

from tqdm.auto import tqdm
import jsonlines


logging.basicConfig(level=logging.INFO, format="get-links: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s")


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True, help="GovInfo API key")
    parser.add_argument("--start-date", required=True, help="Start date in ISO8601 format (yyyy-MM-dd'T'HH:mm:ss'Z')")
    parser.add_argument("--output-dir", required=True, help="Path to output directory")
    parser.add_argument("--workers", type=int, default=10, help="Number of threads")
    parser.add_argument("--collections", nargs="+", default=["BILLS",
                                                             "BUDGET",
                                                             "CDIR",
                                                             "CFR",
                                                             "CPD",
                                                             "CRI",
                                                             "CZIC",
                                                             "GAOREPORTS",
                                                             "GOVPUB",
                                                             "GPO",
                                                             "HJOURNAL",
                                                             "HOB",
                                                             "PAI",
                                                             "PLAW",
                                                             "USCODE"])
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


def get_collections(api_key):
    response = api_query("https://api.govinfo.gov/collections", headers={"accept": "application/json"}, params={"api_key": args.api_key})
    if response.status_code == 200:
        output = response.json()
        for record in output["collections"]:
            yield record["collectionCode"]
    else:
        logging.error(f"get_collections received status code {response.status_code}")


def get_packages(api_key, collections, start_date, package_queue):
    url = f"https://api.govinfo.gov/published/{start_date}"
    offset_mark = "*"
    pbar = tqdm(desc="Producer")
    while url is not None:
        response = api_query(url, 
                             headers={"accept": "application/json"}, 
                             params={"api_key": args.api_key, "offsetMark": offset_mark, "pageSize": 1000, "collection": ",".join(collections)})
        if response.status_code == 200:
            output = response.json()

            for record in output["packages"]:
                package_queue.put(record)
                pbar.update(1)

            url = output["nextPage"] 
            offset_mark = None
            # Prevent too many API requests in a short period of time
            time.sleep(5)
        else:
            logging.error(f"get_packages received status code {response.status_code} for query {url}")
            break
    
    package_queue.put(None)


def get_file_links(api_key, package):
    package_id = package["packageId"]
    response = api_query(f"https://api.govinfo.gov/packages/{package_id}/summary", headers={"accept": "application/json"}, params={"api_key": args.api_key})
    if response.status_code == 200:
        output = response.json()
        return output.get("download")
    return None


def get_package_metadata(api_key, package_queue, metadata_queue):
    pbar = tqdm(desc="Consumer")
    while True:
        package = package_queue.get()
        if package is None:
            package_queue.put(None)
            metadata_queue.put(None)
            break

        record = {
                "title": package.get("title"),
                "package_id": package.get("packageId"),
                "date": package.get("dateIssued"),
                "category": package.get("category"),
                "author": package.get("governmentAuthor1"),
                "publisher": package.get("publisher"),
                "links": get_file_links(api_key, package) 
                }
        metadata_queue.put(record)
        pbar.update(1)
        

def write_metadata(output_dir, metadata_queue):
    with jsonlines.open(os.path.join(output_dir, "links.jsonl"), mode="w") as writer:
        pbar = tqdm(desc="Writer")
        while True:
            metadata = metadata_queue.get()
            if metadata is None:
                metadata_queue.task_done()
                break
            
            writer.write(metadata)
            pbar.update(1)


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)
    
    package_queue = queue.Queue()
    metadata_queue = queue.Queue()

    with ThreadPoolExecutor(max_workers=args.workers+2) as executor:
        executor.submit(get_packages, args.api_key, args.collections, args.start_date, package_queue)
        
        for _ in range(args.workers):
            executor.submit(get_package_metadata, args.api_key, package_queue, metadata_queue)
        
        executor.submit(write_metadata, args.output_dir, metadata_queue)

        metadata_queue.join()
    

if __name__ == "__main__":
    args = parse_args()
    main(args)
