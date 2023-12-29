"""Build index of Biodiversity Heritage Library books"""

import argparse
import tarfile
import os
import logging
import json
from collections import defaultdict

from tqdm.auto import tqdm


logging.basicConfig(level=logging.INFO, format="extract-files: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s")


SOURCE_NAME = "biodiversity-heritage-library"

def parse_args():
    parser = argparse.ArgumentParser("Biodiversity Heritage Library file extractor")
    parser.add_argument("--index-file", default=f"data/{SOURCE_NAME}/raw/index.json", help="Path to JSON index")
    parser.add_argument("--whitelist-file", default="bhl/license_whitelist.json", help="Path to JSON file of whitelisted license strings")
    parser.add_argument("--content-file", default=f"data/{SOURCE_NAME}/raw/data/bhl-ocr-20230823.tar.bz2", help="Path to tar-ed and bz2 compressed content file")
    parser.add_argument("--output-dir", default=f"data/{SOURCE_NAME}/raw/extracted_data", help="Path to output directory")
    return parser.parse_args()


def main(args):
    index = {}

    logging.info(f"Loading index file from {args.index_file}")
    with open(args.index_file, "r") as f:
        index = json.load(f)
    
    logging.info(f"Loading license whitelist file from {args.whitelist_file}")
    with open(args.whitelist_file, "r") as f:
        whitelist = json.load(f)
    
    logging.info(f"Loading content file from {args.content_file}")
    content_file = tarfile.open(args.content_file, "r:bz2")

    logging.info("Constructing list of all whitelisted items")
    whitelisted_items = set(sum([[uri.split("/")[-1].zfill(6) for uri in index[license]] for license in whitelist], start=[]))
    logging.info(f"Found {len(whitelisted_items)} whitelisted items")
    
    num_extracted_files = 0
    extracted_size = 0
    pbar = tqdm(content_file)
    for item_info in pbar:
        if not item_info.isfile():
            continue
        item_id = item_info.path.split("/")[2]
        if item_id in whitelisted_items:
            content_file.extract(item_info, path=args.output_dir) 
            num_extracted_files += 1
            extracted_size += item_info.size
             
        pbar.set_postfix({"Extracted Files": num_extracted_files, "Extracted Size": f"{extracted_size / 2**30:.3f} GB"})
    

if __name__ == "__main__":
    args = parse_args()
    main(args)
