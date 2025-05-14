"""Convert python pep's into the dolma format."""

import argparse
import glob
import os
import re
from datetime import datetime

import docutils.core

from common_pile import logs
from common_pile.licenses import PermissiveLicenses
from common_pile.write import to_dolma

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


def check_for_open_pub_license(text):
    doc = docutils.core.publish_doctree(
        text,
        settings_overrides={
            "file_insertion_enabled": False,
            "report_level": 5,
            "halt_level": 5,
        },
    )
    for cr in doc.findall(
        lambda s: s.tagname == "section"
        and "copyright" in [name.lower() for name in s["names"]]
    ):
        for paragraph in cr.findall(lambda p: p.tagname == "paragraph"):
            copyright_text = paragraph.rawsource.lower()
            if "open publication license" in copyright_text:
                return True
            if "https://spdx.org/licenses/OPUBL-1.0.html" in copyright_text:
                return True
            if "http://www.opencontent.org/openpub/" in copyright_text:
                return True
    return False


def format_dolma(path, source_name: str = SOURCE_NAME):
    with open(path) as f:
        text = f.read()
    if check_for_open_pub_license(text):
        logger = logs.get_logger()
        logger.warning(f"Skipping {path} as it is Open Publication License.")
        return None
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
    pep_files = (p for p in pep_files if p is not None)
    to_dolma(pep_files, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging()
    main(args)
