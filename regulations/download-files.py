"""Download Regulations.gov documents"""

import argparse
import itertools
import json
import os

import requests
from common_pile import logs
from tqdm.auto import tqdm


def parse_args():
    parser = argparse.ArgumentParser(description="Download Regulations.gov documents")
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
        default=tuple(
            [
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
            ]
        ),
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

    args.file_types = set(args.file_types)
    for year, agency in itertools.product(args.years, args.agencies):
        os.makedirs(os.path.join(args.output_dir, year, agency), exist_ok=True)
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

        # Iterate through index---each key is a document ID, and each value is a list of metadata dictionaries containing lists of content files
        pbar = tqdm(index.items())
        for doc_id, metadatas in pbar:
            for metadata in metadatas:
                for file in metadata["Content Files"]:
                    output_file = os.path.join(
                        output_dir, f"{doc_id}{file['File Type']}"
                    )
                    if os.path.exists(output_file):
                        logger.debug(f"File {output_file} already exists")
                        pbar_stats["Already Exists"] += 1
                        continue
                    if file["File Type"] not in args.file_types:
                        logger.debug(
                            f"Skipping {output_file} -- wrong file type: {file['File Type']}"
                        )
                        pbar_stats["Wrong File Type"] += 1
                        continue
                    ret = download_file(file["URL"], output_file)
                    if ret == 200:
                        logger.debug(f"Downloaded {output_file}")
                        pbar_stats["Downloaded"] += 1
                    else:
                        logger.error(
                            f"Failed to download {output_file} -- status code: {ret}"
                        )
                        pbar_stats["Errors"] += 1
                    pbar.set_postfix(pbar_stats)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("regulations")
    main(args)
