"""Convert python pep's into the dolma format."""

import argparse
import glob
import os
import re
from datetime import datetime

from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

SOURCE_NAME = "python-peps"

parser = argparse.ArgumentParser(description="Convert peps to dolma.")
parser.add_argument("--peps", required=True, help="The path to the cloned pep repo.")
parser.add_argument(
    "--output_dir",
    default="data/peps-dolma/raw/documents/",
    help="Where the dolma formatted data goes.",
)
parser.add_argument(
    "--filename", default="peps.jsonl.gz", help="The base filename for shards."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)


def extract_pep_number(file_name):
    if m := re.match(r"^pep-(?P<num>\d{4}).rst$", file_name):
        return m.group("num")


def format_dolma(path, source_name: str = SOURCE_NAME):
    with open(path) as f:
        text = f.read()
    pep_number = extract_pep_number(os.path.basename(path))
    return {
        "id": pep_number,
        "text": text,
        "source": source_name,
        "added": datetime.utcnow().isoformat(),
        "created": None,
        "metadata": {
            "license": str(PermissiveLicenses.PD),
            "url": f"https://peps.python.org/pep-{pep_number}/",
            "authors": None,
            "pep_number": pep_number,
        },
    }


def main(args):
    pep_files = glob.iglob(os.path.join(args.peps, "peps", "pep-*.rst"))
    pep_files = map(format_dolma, pep_files)
    to_dolma(pep_files, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging()
    main(args)
