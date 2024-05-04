"""Download Data Provenance Initative data"""

import argparse
import gzip
import json
import logging
import multiprocessing
import os
import tarfile
import typing
from collections import defaultdict

import jsonlines
import pandas as pd
from datasets import load_dataset
from tqdm.auto import tqdm

from data_provenance.constants import HF_MAPPING
from licensed_pile.logs import configure_logging, get_logger


def parse_args():
    parser = argparse.ArgumentParser(description="Data Provenance Data Downloader")
    parser.add_argument(
        "--hf",
        default="DataProvenanceInitiative/Commercially-Verified-Licenses",
        help="The label for the HuggingFace dataset that can be used in HuggingFace's load_dataset()",
    )
    parser.add_argument(
        "--include",
        default="./data_provenance/include.csv",
        help="Path to csv file with `Collection Name, Dataset ID` we will include",
    )
    parser.add_argument(
        "--outdir", default="data/raw-data-provenance", help="Path to output directory"
    )
    return parser.parse_args()


def write_jsonl_gz(
    data,
    outpath,
):
    dirname = os.path.dirname(outpath)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    with gzip.open(outpath, "wb") as fp:  # Open file in binary write mode
        data_bytes = (
            b"\n".join(json.dumps(d).encode() for d in data) + b"\n"
        )  # Encode strings to bytes
        fp.write(data_bytes)


def main(args):
    logger = get_logger()
    logger.info(f"Filtering to just the datasets in {args.include}")

    include_df = pd.read_csv(args.include)
    include_collections = list(set(include_df["Collection"]))
    include_dset_ids = set(include_df["Dataset ID"])

    for collection in include_collections:
        folder_name = HF_MAPPING[collection]
        subset = load_dataset(
            args.hf,
            split="train",
            num_proc=os.cpu_count(),
            revision="main",
            data_files=f"data/{folder_name}/*.jsonl",
        ).to_list()
        exs = [ex for ex in subset if ex["user_parent"] in include_dset_ids]
        savepath = os.path.join(args.outdir, f"{folder_name}.jsonl.gz")
        write_jsonl_gz(exs, savepath)
        logger.info(f"Saving {len(exs)} examples to {savepath}")


if __name__ == "__main__":
    args = parse_args()
    main(args)
