"""Build indexes of document URLs from a Regulations.gov bulk download files"""

import argparse
import csv
import itertools
import json
import os
import sys
from collections import defaultdict

import parsing
from common_pile import logs
from tqdm.auto import tqdm

csv.field_size_limit(sys.maxsize)


def parse_args():
    parser = argparse.ArgumentParser(description="Regulations.gov index builder")
    parser.add_argument("--input-dir", required=True, help="Path to raw data")
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
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing files in output directory",
    )
    return parser.parse_args()


def main(args):
    logger = logs.get_logger("regulations")
    for year in args.years:
        os.makedirs(os.path.join(args.output_dir, year), exist_ok=True)

    for year, agency in itertools.product(args.years, args.agencies):
        input_file = os.path.join(args.input_dir, year, f"{agency}.csv")
        output_file = os.path.join(args.output_dir, year, f"{agency}.json")

        if not os.path.exists(input_file):
            logger.info(f"Skipping {agency} data from {year} -- no input file")
            continue

        if os.path.exists(output_file) and not args.overwrite:
            logger.info(f"Skipping {agency} data from {year} -- existing output file")
            continue

        logger.info(f"Processing {agency} data from {year}")

        index = defaultdict(list)
        num_skipped = 0
        num_parsed = 0
        with open(input_file, "r", newline="") as f:
            reader = csv.DictReader(f, delimiter=",")
            pbar = tqdm(reader)
            for record in pbar:
                parsed_record = parsing.parse_record(record)
                if parsed_record is not None:
                    doc_id, parsed_record = parsed_record
                    index[doc_id].append(parsed_record)
                    num_parsed += 1
                else:
                    num_skipped += 1
                pbar.set_postfix({"Parsed": num_parsed, "Skipped": num_skipped})

        with open(output_file, "w") as f:
            json.dump(index, f, indent=4)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("regulations")
    main(args)
