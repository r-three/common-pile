"""Convert the downloaded data into the dolma sharded jsonl.gz format.

We use `download.py` to save our own intermediate copy of the data, before
preparing for dolma, in this file.
"""

import argparse
import functools
import glob
import gzip
import itertools
import json
import os
import re
from datetime import datetime

import jsonlines
import pandas as pd
from constants import HF_MAPPING
from tqdm import tqdm

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.logs import configure_logging, get_logger
from licensed_pile.write import to_dolma

LICENSE_MAPPER = {
    "CDLA Permissive 1.0": PermissiveLicenses.CDLA_P,
    "MIT License": PermissiveLicenses.MIT,
    "CC BY 4.0": PermissiveLicenses.CC_BY,
    "CC0 1.0": PermissiveLicenses.CC0,
    "BSD 2-Clause License": PermissiveLicenses.BSD_2,
    "BSD 3-Clause License": PermissiveLicenses.BSD_3,
    "Apache License 2.0": PermissiveLicenses.APACHE_2,
    "ISC License": PermissiveLicenses.ISC,
    "CC BY-SA": PermissiveLicenses.CC_BY_SA,
    "CC BY 3.0": PermissiveLicenses.CC_BY_3,
    "CC BY-SA 3.0": PermissiveLicenses.CC_BY_SA_3,
    "Artistic License 2.0": PermissiveLicenses.ARTISTIC_2,
    "CC BY-SA 4.0": PermissiveLicenses.CC_BY_SA,
}

parser = argparse.ArgumentParser(
    description="Collect Data Provenance datasets into Dolma format."
)
parser.add_argument(
    "--indir",
    default="data/raw-data-provenance",
    help="Path to our directory of raw datasets.",
)
parser.add_argument(
    "--outdir",
    default="data/data-provenance/v0/documents/",
    help="Where the dolma formatted data goes.",
)
parser.add_argument(
    "--include",
    default="include.csv",
    help="The csv with metadata on data provenance datasets.",
)
parser.add_argument(
    "--filename", default="dpi.jsonl.gz", help="The base filename for our datasets."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)

SOURCE_NAME = "Data Provenance Initiative"


def unique_dpi_dataset_names(folder_path):
    unique_doc_ids = set()

    jsonl_paths = os.path.join(folder_path, "**/*.jsonl.gz")
    jsonl_files = glob.glob(jsonl_paths, recursive=True)

    for file_path in tqdm(jsonl_files, desc="Processing files"):
        with gzip.open(file_path, "rt") as f:
            for line in f:
                data = json.loads(line)
                doc_id = data["metadata"]["dataset_id"]
                unique_doc_ids.add(doc_id)
    return unique_doc_ids


def listdir_nohidden(path):
    """Returns all non-hidden files within a directory, raises ValueError if the path is invalid."""
    if not os.path.exists(path) or not os.path.isdir(path):
        raise ValueError(
            f"Provided path '{path}' is either not a directory or does not exist."
        )
    return [os.path.join(path, f) for f in os.listdir(path) if not f.startswith(".")]


def read_jsonl_gz(inpath: str):
    with gzip.open(inpath, "rb") as fp:
        return [json.loads(l) for l in fp]


def extract_licenses(license_list, gh_license):
    license_set = set()
    for license_dict in eval(license_list):
        if license_dict["License"] != "Unspecified":
            license_set.add(str(LICENSE_MAPPER[license_dict["License"]]))
    if gh_license:
        license_set = list(license_set) + [str(LICENSE_MAPPER[gh_license])]
    return license_set


def file_to_dolma(path: str, include_df: str, source_name: str = SOURCE_NAME):
    logger = get_logger()
    logger.info(f"Converting {path} to the dolma format.")

    valid_ids = set(include_df["Dataset ID"])

    dset_to_licenses = {
        row["Dataset ID"]: extract_licenses(row["Licenses"], row["GitHub License"])
        for _, row in include_df.iterrows()
    }
    dset_to_license_urls = {
        row["Dataset ID"]: entry["License URL"]
        for _, row in include_df.iterrows()
        for entry in eval(row["Licenses"])
    }
    dset_to_langs = {
        row["Dataset ID"]: eval(row["Languages"]) for _, row in include_df.iterrows()
    }
    dset_to_urls = {
        row["Dataset ID"]: row["Dataset URL"] for _, row in include_df.iterrows()
    }

    dset_collection = read_jsonl_gz(path)

    results = []
    for i, ex in enumerate(dset_collection):
        dataset_id = ex["dataset"]

        assert (
            dataset_id in valid_ids
        ), f"Dataset ID '{dataset_id}' not found in include.csv"

        license_names = dset_to_licenses[dataset_id]
        langs = dset_to_langs[dataset_id]
        url = dset_to_urls[dataset_id]
        license_urls = dset_to_license_urls[dataset_id]

        input_text = ex["inputs"]
        target_text = ex.get("labels", ex.get("targets", ""))
        # If target_text isn't found, the strip will remove the extra newline
        text = f"{input_text}\n{target_text}".strip()
        results.append(
            {
                "id": f"{ex['dataset']}-{i}",
                "text": text,
                "source": source_name,
                "added": datetime.utcnow().isoformat(),
                "metadata": {
                    "license": sorted(license_names),
                    "license_url": license_urls,
                    "language": langs,
                    "url": url,
                    "dataset_id": dataset_id,
                    "response": target_text,
                },
            }
        )

    return results


def main(args):
    logger = get_logger()

    os.makedirs(args.outdir, exist_ok=True)

    include_df = pd.read_csv(args.include).fillna("")

    paths = listdir_nohidden(args.indir)
    examples = itertools.chain(
        *(file_to_dolma(path, include_df=include_df) for path in paths)
    )
    to_dolma(examples, args.outdir, args.filename, args.shard_size)

    valid_dpi_ids = set(include_df["Dataset ID"])
    unique_dpi_ids = unique_dpi_dataset_names(args.outdir)
    unseen_dpi_ids = valid_dpi_ids - unique_dpi_ids
    logger.info(f"Unseen Datasets: {len(unseen_dpi_ids)} | {unseen_dpi_ids}")
    logger.info(f"Total Unique Datasets: {len(unique_dpi_ids)}")


if __name__ == "__main__":
    args = parser.parse_args()
    configure_logging()
    main(args)
