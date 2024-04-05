"""Download Data Provenance Initative data"""

import argparse
import tarfile
import os
import logging
import json
import jsonlines
import gzip
import multiprocessing
import pandas as pd
import typing
from collections import defaultdict
from datasets import load_dataset

from tqdm.auto import tqdm

logging.basicConfig(level=logging.INFO, format="build-index: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s")


def parse_args():
    parser = argparse.ArgumentParser("Data Provenance Data Downloader")
    parser.add_argument("--hf", default="DataProvenanceInitiative/Commercially-Verified-Licenses", help="HuggingFace path")
    parser.add_argument("--include", default="./include.txt", help="Path to text file with dataset names we will include")
    parser.add_argument("--outdir", default="./data", help="Path to output directory")
    return parser.parse_args()


def write_jsonl(
    data: typing.Union[pd.DataFrame, typing.List[typing.Dict]], 
    outpath: str, 
    compress: bool=False, 
    dumps=None,
):
    dirname = os.path.dirname(outpath)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    if isinstance(data, list):
        if compress:
            with gzip.open(outpath, 'wb') as fp:
                json_writer = jsonlines.Writer(fp)#, dumps=dumps)
                json_writer.write_all(data)
        else:
            with open(outpath, "wb") as fp:
                json_writer = jsonlines.Writer(fp) #, dumps=dumps)
                json_writer.write_all(data)
    else: # Must be dataframe:
        data.to_json(outpath, orient="records", lines=True, compression="gzip" if compress else "infer")


def main(args):
    logging.info(f"Loading dataset from {args.hf}")
    dataset = load_dataset(args["hf"], revision="main")

    logging.info(f"Filtering to just the datasets in {args.include}")
    filtered_dataset = dataset.filter(lambda x: x["user_parent"] not in args["include"])

    def create_row(ex):
        ex["text"] = ex["inputs"] + ex["labels"]
        return ex

    logging.info(f"Transforming data...")
    final_data = filtered_dataset.map(create_row)

    savepath = os.path.join(args["outdir"], "data.jsonl.gz")
    logging.info(f"Saving data to {savepath}")
    write_jsonl(final_data, savepath, compress=True)



if __name__ == "__main__":
    args = parse_args()
    main(args)
