"""Download Regulations.gov documents from indexes containing document URLs"""

import argparse
import itertools
import json
import os

import requests
from tqdm.auto import tqdm

from licensed_pile import logs


def parse_args():
    parser = argparse.ArgumentParser("Regulations.gov index builder")
    parser.add_argument(
        "--input-dir", required=True, help="Path to directory containing indexes"
    )
    parser.add_argument("--output-dir", required=True, help="Path to output directory")
    parser.add_argument(
        "--years",
        nargs="+",
        default=list(map(str, range(2000, 2024))),
        help="Years of data to process",
    )
    parser.add_argument(
        "--agencies",
        nargs="+",
        default=[
            "bis",
            "dot",
            "epa",
            "faa",
            "fda",
            "fema",
            "ferc",
            "fmcsa",
            "fra",
            "nhtsa",
            "osha",
            "phmsa",
            "sec",
            "uscg",
        ],
        help="Agencies to process",
    )
    parser.add_argument(
        "--file-types",
        nargs="+",
        default=[".txt", ".htm", ".doc", ".docx"],
        help="File types to download",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing files in output directory",
    )
    return parser.parse_args()


def download_file(url, filename):
    try:
        response = requests.get(url)
    except:
        return -1
    if response.status_code == 200:
        with open(filename, "wb") as file:
            file.write(response.content)
        return response.status_code
    else:
        return -1


def main(args):
    logger = logs.get_logger("regulations")
    for year in args.years:
        for agency in args.agencies:
            os.makedirs(os.path.join(args.output_dir, year, agency), exist_ok=True)

    for year, agency in itertools.product(args.years, args.agencies):
        input_file = os.path.join(args.input_dir, year, f"{agency}.json")
        output_dir = os.path.join(args.output_dir, year, agency)

        if not os.path.exists(input_file):
            logger.info(f"Skipping {agency} data from {year} -- no input file")
            continue

        logger.info(f"Processing {agency} data from {year}")

        with open(input_file, "r") as f:
            index = json.load(f)

        pbar_stats = {
            "Downloaded": 0,
            "Errors": 0,
            "Already Exists": 0,
            "Wrong File Type": 0,
        }
        pbar = tqdm(index.items())
        for doc_id, metadatas in pbar:
            for metadata in metadatas:
                for file in metadata["Content Files"]:
                    output_file = os.path.join(
                        output_dir, f"{doc_id}{file['File Type']}"
                    )
                    if os.path.exists(output_file):
                        pbar_stats["Already Exists"] += 1
                        continue
                    if file["File Type"] not in args.file_types:
                        pbar_stats["Wrong File Type"] += 1
                        continue
                    ret = download_file(file["URL"], output_file)
                    if ret == 200:
                        pbar_stats["Downloaded"] += 1
                    else:
                        pbar_stats["Errors"] += 1
                    pbar.set_postfix(pbar_stats)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("regulations")
    main(args)
