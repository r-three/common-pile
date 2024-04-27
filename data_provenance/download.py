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

# logger = configure_logging()


def parse_args():
    parser = argparse.ArgumentParser("Data Provenance Data Downloader")
    parser.add_argument(
        "--hf",
        default="DataProvenanceInitiative/Commercially-Verified-Licenses",
        help="HuggingFace path",
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


def write_jsonl(
    data,
    outpath,
    compress: bool = False,
    dumps=None,
):
    dirname = os.path.dirname(outpath)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    if isinstance(data, list):
        if compress:
            with gzip.open(outpath, "wb") as fp:
                json_writer = jsonlines.Writer(fp)  # , dumps=dumps)
                json_writer.write_all(data)
        else:
            with open(outpath, "wb") as fp:
                json_writer = jsonlines.Writer(fp)  # , dumps=dumps)
                json_writer.write_all(data)
    else:  # Must be dataframe:
        data.to_json(
            outpath,
            orient="records",
            lines=True,
            compression="gzip" if compress else "infer",
        )


def main(args):
    # logger.info(f"Loading dataset from {args.hf}")
    # dataset = load_dataset(args.hf, revision="main")
    logger = get_logger()
    logger.info(f"Filtering to just the datasets in {args.include}")

    include_df = pd.read_csv(args.include)  # , sep="\t")
    include_collections = list(set(include_df["Collection"]))
    include_dset_ids = list(set(include_df["Dataset ID"]))
    # filtered_dataset = dataset.filter(lambda x: x["user_parent"] not in include_dset_ids)

    for collection in include_collections:
        folder_name = HF_MAPPING[collection]
        # dataset = load_dataset("DataProvenanceInitiative/Commercially-Verified-Licenses")
        subset = load_dataset(
            args.hf,
            split="train",
            num_proc=os.cpu_count(),
            revision="main",
            data_files=f"data/{folder_name}/*.jsonl",
        ).to_list()
        exs = [ex for ex in subset if ex["user_parent"] in include_dset_ids]
        # subset = subset[subset["user_parent"].isin(include_dset_ids)]
        savepath = os.path.join(args.outdir, f"{folder_name}.jsonl.gz")
        write_jsonl(exs, savepath, compress=True)
        logger.info(f"Saving {len(exs)} examples to {savepath}")


if __name__ == "__main__":
    args = parse_args()
    main(args)
