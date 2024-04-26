"""Convert the index+data into the dolma sharded jsonl.gz format."""

import argparse
import functools
import json
import operator as op
import pandas as pd
import gzip
import jsonlines
import os
from datetime import datetime

from data_provenance.constants import HF_MAPPING
from licensed_pile.licenses import PermissiveLicenses
# from licensed_pile.write import to_dolma

LICENSE_MAPPER = {
    "CDLA Sharing 1.0": PermissiveLicenses.CDLA,
    "MPL 2.0": PermissiveLicenses.MPL,
    "CDLA Permissive 1.0": PermissiveLicenses.CDLA_P,
    "Custom": PermissiveLicenses.CUSTOM,
    "No License": PermissiveLicenses.NO_LICENSE,
    "OANC": PermissiveLicenses.OANC,
    "GNU General Public License v3.0": PermissiveLicenses.GPL_V3,
    "MIT License": PermissiveLicenses.MIT,
    "CC BY 4.0": PermissiveLicenses.CC_BY,
    "CC0 1.0": PermissiveLicenses.CC0,
    "BSD 2-Clause License": PermissiveLicenses.BSD,  # Assuming "BSD" refers to both 2-Clause and 3-Clause
    "Apache License 2.0": PermissiveLicenses.APACHE_2,
    "ISC License": PermissiveLicenses.ISC,
    "EPL 1.0": PermissiveLicenses.EPL,
    "GNU General Public License v2.0": PermissiveLicenses.GPL_V2,
    "LGPL 2.1": PermissiveLicenses.LGPL_2_1,
    "CC BY-SA": PermissiveLicenses.CC_BY_SA,
    "C-UDA": PermissiveLicenses.C_UDA,
    "CC BY 3.0": PermissiveLicenses.CC_BY_3,
    "CC BY-SA 3.0": PermissiveLicenses.CC_BY_SA_3,
    "Artistic License 2.0": PermissiveLicenses.ARTISTIC_2,
    "CC BY-SA 4.0": PermissiveLicenses.CC_BY_SA,
    "BSD 3-Clause License": PermissiveLicenses.BSD
}

parser = argparse.ArgumentParser(description="Collect Data Provenance datasets into Dolma format.")
parser.add_argument(
    "--indir", default="data/raw-data-provenance", help="Path to our directory of raw datasets."
)
parser.add_argument(
    "--outdir",
    default="data/data-provenance/raw/documents/",
    help="Where the dolma formatted data goes.",
)
parser.add_argument(
    "--include", default="data_provenance/include.csv", help="The csv with metadata on data provenance datasets."
)
parser.add_argument(
    "--filename", default="dpi.jsonl.gz", help="The base filename for our datasets."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)

SOURCE_NAME = "Data Provenance Initiative"


def listdir_nohidden(path):
    """Returns all non-hidden files within a directory."""
    assert os.path.exists(path) and os.path.isdir(path)
    return [os.path.join(path, f) for f in os.listdir(path) if not f.startswith(".")]

def read_jsonl(inpath: str):
    if inpath[-2:] in ["gz", "gzip"]:
        with gzip.open(inpath, 'rb') as fp:
            j_reader = jsonlines.Reader(fp)
            return [l for l in j_reader]
    else:
        with open(inpath, "rb") as fp:
            j_reader = jsonlines.Reader(fp)
            return [l for l in j_reader]

def extract_licenses(license_list, gh_license):
    license_set = set()
    for license_dict in eval(license_list):
        if license_dict['License'] != "Unspecified":
            license_set.add(license_dict['License'])
    return list(license_set) + [gh_license]


def format_dolma(path: str, include_df: str, source_name: str = SOURCE_NAME):
    dset_to_license = {row["Dataset ID"]: extract_licenses(row["Licenses"], row["GitHub License"])[0] for i, row in include_df.iterrows()}
    dset_to_lang = {row["Dataset ID"]: eval(row["Languages"])[0] for i, row in include_df.iterrows()}
    dset_to_url = {row["Dataset ID"]: row["Dataset URL"] for i, row in include_df.iterrows()}

    dset_collection = read_jsonl(path)

    results = []
    for i, ex in enumerate(dset_collection):
        license_name = dset_to_license[ex["user_parent"]]
        lang = dset_to_lang[ex["user_parent"]]
        url = dset_to_url[ex["user_parent"]]
        results.append({
            "id": f"{ex['user_parent']}-{i}",
            "text": ex["inputs"] + "\n" + ex["labels"],
            "source": source_name,
            "added": datetime.utcnow().isoformat(),
            "metadata": {
                "license": license_name,
                "language": lang,
                "url": url,
                "title": "",
            },
        })
    return results


def main(args):
    os.makedirs(args.outdir, exist_ok=True)

    include_df = pd.read_csv(args.include)

    paths = listdir_nohidden(args.indir)
    examples = []
    for path in paths:
        examples.extend(format_dolma(path, include_df=include_df))
    to_dolma(examples, args.outdir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
