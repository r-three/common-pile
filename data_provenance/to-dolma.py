"""Convert the downloaded data into the dolma sharded jsonl.gz format.

We use `download.py` to save our own intermediate copy of the data, before
preparing for dolma, in this file.
"""

import argparse
import functools
import gzip
import json
import operator as op
import os
from datetime import datetime

import jsonlines
import pandas as pd

from data_provenance.constants import HF_MAPPING
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

LICENSE_MAPPER = {
    "CDLA Sharing 1.0": PermissiveLicenses.CDLA,
    "MPL 2.0": PermissiveLicenses.MPL,
    "CDLA Permissive 1.0": PermissiveLicenses.CDLA_P,
    "Custom": PermissiveLicenses.CUSTOM,
    "No License": PermissiveLicenses.NO_LICENSE,
    "OANC": PermissiveLicenses.OANC,
    "MIT License": PermissiveLicenses.MIT,
    "CC BY 4.0": PermissiveLicenses.CC_BY,
    "CC0 1.0": PermissiveLicenses.CC0,
    "BSD 2-Clause License": PermissiveLicenses.BSD_2,
    "BSD 3-Clause License": PermissiveLicenses.BSD_3,
    "Apache License 2.0": PermissiveLicenses.APACHE_2,
    "ISC License": PermissiveLicenses.ISC,
    "EPL 1.0": PermissiveLicenses.EPL,
    "LGPL 2.1": PermissiveLicenses.LGPL_2_1,
    "CC BY-SA": PermissiveLicenses.CC_BY_SA,
    "C-UDA": PermissiveLicenses.C_UDA,
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
    default="data/data-provenance/raw/documents/",
    help="Where the dolma formatted data goes.",
)
parser.add_argument(
    "--include",
    default="data_provenance/include.csv",
    help="The csv with metadata on data provenance datasets.",
)
parser.add_argument(
    "--filename", default="dpi.jsonl.gz", help="The base filename for our datasets."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)

SOURCE_NAME = "Data Provenance Initiative"


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
            license_set.add(license_dict["License"])
    return list(license_set) + [gh_license]


def file_to_dolma(path: str, include_df: str, source_name: str = SOURCE_NAME):
    dset_to_license = {
        row["Dataset ID"]: extract_licenses(row["Licenses"], row["GitHub License"])[0]
        for _, row in include_df.iterrows()
    }
    dset_to_lang = {
        row["Dataset ID"]: eval(row["Languages"])[0] for _, row in include_df.iterrows()
    }
    dset_to_url = {
        row["Dataset ID"]: row["Dataset URL"] for _, row in include_df.iterrows()
    }

    dset_collection = read_jsonl_gz(path)

    results = []
    for i, ex in enumerate(dset_collection):
        license_name = str(LICENSE_MAPPER[dset_to_license[ex["user_parent"]]])
        lang = dset_to_lang[ex["user_parent"]]
        url = dset_to_url[ex["user_parent"]]
        results.append(
            {
                "id": f"{ex['user_parent']}-{i}",
                "text": ex["inputs"],
                "response": ex["labels"],
                "source": source_name,
                "added": datetime.utcnow().isoformat(),
                "metadata": {
                    "license": license_name,
                    "language": lang,
                    "url": url,
                    "dataset_id": ex["user_parent"],
                },
            }
        )
    return results


def main(args):
    os.makedirs(args.outdir, exist_ok=True)

    include_df = pd.read_csv(args.include)

    paths = listdir_nohidden(args.indir)
    examples = []
    for path in paths:
        examples.extend(file_to_dolma(path, include_df=include_df))
    to_dolma(examples, args.outdir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
