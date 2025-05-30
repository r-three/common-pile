import argparse
import datetime
import glob
import json
import logging
import os
import sys
import re

import pandas as pd
from tqdm import tqdm

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma



logging.basicConfig(
    level=logging.INFO,
    format="to-dolma: [%(asctime)s] [%(funcName)s] %(levelname)s - %(message)s",
)


SOURCE_NAME = "doab"

parser = argparse.ArgumentParser(description="Convert data to dolma.")
parser.add_argument(
    "--metadata", type=str, help="Path to metadata file"
)
parser.add_argument(
    "--input-files", nargs="+", help="Input files"
)
parser.add_argument(
    "--output-dir", type=str, help="Path to output directory"
)
parser.add_argument(
    "--filename", default="doab.json.gz", help="The base filename for the BHL data"
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)
args = parser.parse_args()


def str_to_license(s):
    if "creativecommons.org/licenses/by/4.0" in s:
        return PermissiveLicenses.CC_BY
    if "creativecommons.org/licenses/by/3.0" in s:
        return PermissiveLicenses.CC_BY_3
    if "creativecommons.org/licenses/by/3.0" in s:
        return PermissiveLicenses.CC_BY_2
    if "creativecommons.org/licenses/by-sa/4.0" in s:
        return PermissiveLicenses.CC_BY_SA
    if "creativecommons.org/licenses/by-sa/3.0" in s:
        return PermissiveLicenses.CC_BY_SA_3
    return None


def get_records():
    metadata = pd.read_csv(args.metadata)
    metadata = metadata.set_index("id")
    
    for file in args.input_files:
        basename = os.path.basename(file)
        id = os.path.splitext(basename)[0]

        metadata_record = metadata.loc[id]
        license = str_to_license(metadata_record["BITSTREAM License"])
        if license is None:
            continue
        
        with open(file, "r") as f:
            lines = f.readlines()
        
        lines = [l for l in lines if len(l) > 0 and not l.startswith("-") and not l.startswith("|")]
        text = "".join(lines)
        
        created = metadata_record["dc.date.available"]
        url = metadata_record["BITSTREAM Download URL"]
        author = metadata_record["dc.contributor.author"]
        title = metadata_record["dc.title"]
        isbn = metadata_record["BITSTREAM ISBN"]
        publisher_name = metadata_record["oapen.relation.isPublishedBy_publisher.name"]

        if publisher_name in ["MDPI - Multidisciplinary Digital Publishing Institute", "IntechOpen"]:
            sections = re.split("\n# ", text)
            for section_idx, section in enumerate(sections):
                if re.match("^#*\s*\**Reference", section):
                    continue
                if len(section) < 50:
                    continue
                yield {
                    "id": f"{id}.{section_idx}",
                    "text": section,
                    "source": SOURCE_NAME,
                    "added": datetime.datetime.utcnow().isoformat(),
                    "created": "" if pd.isna(created) else created,
                    "metadata": {
                        "license": str(license),
                        "book_id": id,
                        "url": "" if pd.isna(url) else url,
                        "author": "" if pd.isna(author) else author,
                        "title": "" if pd.isna(title) else title,
                        "publisher": "" if pd.isna(publisher_name) else publisher_name,
                        "isbn": "" if pd.isna(isbn) else isbn,
                        "section_idx": section_idx
                    },
                }
     
        else:
            yield {
                "id": id,
                "text": text,
                "source": SOURCE_NAME,
                "added": datetime.datetime.utcnow().isoformat(),
                "created": "" if pd.isna(created) else created,
                "metadata": {
                    "license": str(license),
                    "book_id": id,
                    "url": "" if pd.isna(url) else url,
                    "author": "" if pd.isna(author) else author,
                    "title": "" if pd.isna(title) else title,
                    "publisher": "" if pd.isna(publisher_name) else publisher_name,
                    "isbn": "" if pd.isna(isbn) else isbn,
                    "section_idx": 0
                },
            }
            

def main(args):
    # Use iterators so we don't have to load the whole dataset in memory.
    to_dolma(get_records(), args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
