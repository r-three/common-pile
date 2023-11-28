"""Build index of Biodiversity Heritage Library books"""

import argparse
import xml.etree.ElementTree as ET
import os
import logging
import json
from collections import defaultdict

from tqdm.auto import tqdm


logging.basicConfig(level=logging.INFO, format="build-index: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s")


def parse_args():
    parser = argparse.ArgumentParser("Biodiversity Heritage Library index builder")
    parser.add_argument("--metadata-file", default="raw_data/bhlitem.mods.xml", help="Path to XML metadata file")
    parser.add_argument("--output-dir", default="./", help="Path to output directory")
    return parser.parse_args()


def main(args):
    index = defaultdict(list)

    logging.info(f"Loading metadata file from {args.metadata_file}")
    metadata = ET.parse(args.metadata_file).getroot()
   
    num_entries = 0
    pbar = tqdm(metadata)
    for entry in pbar:
        uri = license_info = None
        for field in entry:
            if field.get("type") == "uri":
                uri = field.text
            if field.get("type") == "useAndReproduction":
                license_info = field.text

            if uri is not None and license_info is not None:
                index[license_info].append(uri)
                num_entries += 1
                break

        pbar.set_postfix({"Entries w/ License Info": num_entries})
    
    logging.info("Computing summary statistics")
    counts = {license: len(uris) for license, uris in index.items()}
    print("\nLicense Summary Statistics:")
    print(json.dumps(dict(sorted(counts.items(), reverse=True, key=lambda entry: entry[1])), indent=4))
    
    logging.info(f"Saving index to {args.output_dir}")
    os.makedirs(args.output_dir, exist_ok=True)
    with open(os.path.join(args.output_dir, "index.json"), "w") as f:
        json.dump(index, f, indent=4)


if __name__ == "__main__":
    args = parse_args()
    main(args)
