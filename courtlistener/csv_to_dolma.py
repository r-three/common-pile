#!/usr/bin/env python
"""
Created by zhenlinx on 01/19/2024
"""
import argparse
import pandas as pd
import glob
import os
import sys
import csv
from datetime import datetime
import logging

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

SOURCE_NAME = "CourtListenerOpinion"

csv.field_size_limit(sys.maxsize)

logging.basicConfig(level=logging.INFO, format="to-dolma: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s")


def make_record_generator(file_path):
    with open(file_path, 'r') as csvfile:
        # Create a CSV reader object
        reader = csv.DictReader(csvfile)

        # Yield a dictionary for each row
        for row in reader:
            # 'row' is a dictionary with column headers as keys

            if not row['plain_text']:
                pass  # TODO load from row["download_url"] if not null
            else:
                yield {
                    "id": row["id"],
                    "text": row["plain_text"],
                    "source": SOURCE_NAME,
                    "added": datetime.utcnow().isoformat(),
                    "metadata": {
                        "license": str(PermissiveLicenses.PD),
                        "url": row["download_url"],
                    },
                }


def main(args):
    # Path to your large CSV file
    # for csv_file_path in glob.iglob(os.path.join(args.data, "*.txt")):
        # file_path = './data/courtlistener/raw/opinions-2022-08-02.csv'
    example_generator = make_record_generator(args.input_file)
    output_file_base_name = os.path.basename(args.input_file).replace('.csv', '.jsonl.gz')
    to_dolma(example_generator, args.output_dir, output_file_base_name, args.shard_size)
    logging.info(f"Saved {args.input_file} as dolma shared files at {args.output_dir}")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert csv data to dolma.")
    # parser.add_argument(
    #     "--data", default=f"data/courtlistener/raw", help="Path to the directory containing raw data."
    # )
    parser.add_argument(
        "--output_dir",
        default=f"data/courtlistener/v0",
        help="Where the dolma formatted data goes."
    )
    parser.add_argument(
        "--input_file", default="./data/courtlistener/raw/opinions-2022-08-02.csv", help="The base filename stores data"
    )
    parser.add_argument(
        "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
    )
    args = parser.parse_args()
    main(args)
