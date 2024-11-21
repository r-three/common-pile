import argparse
import datetime
import itertools
import json
import os

from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

SOURCE_NAME = "regulations"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file-dir",
        required=True,
        help="Path to directory containing downloaded and converted files",
    )
    parser.add_argument(
        "--index-dir",
        required=True,
        help="Path to directory containing metadata indexes",
    )
    parser.add_argument(
        "--output-dir",
        default=f"data/{SOURCE_NAME}/v0",
        help="Path to output directory",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        default=list(map(str, range(2000, 2024))),
        help="Years of data to process",
    )
    parser.add_argument(
        "--agencies",
        nargs="+",
        default=[
            "bis",
            "dot",
            "epa",
            "faa",
            "fda",
            "fema",
            "ferc",
            "fmcsa",
            "fra",
            "nhtsa",
            "osha",
            "phmsa",
            "sec",
            "uscg",
        ],
        help="Agencies to process",
    )
    parser.add_argument(
        "--filename",
        default="regulations.jsonl.gz",
        help="The base filename for the Regulations.gov Dolma dataset",
    )
    parser.add_argument(
        "--shard-size", type=int, default=1, help="Size, in GB, for each shard"
    )
    parser.add_argument("--workers", type=int, default=10, help="Number of threads")
    args = parser.parse_args()
    return args


def generate_records(args):
    for year, agency in itertools.product(args.years, args.agencies):
        index_path = os.path.join(args.index_dir, year, f"{agency}.json")
        if not os.path.exists(index_path):
            continue
        with open(index_path, "r") as f:
            index = json.load(f)

        for doc_id, metadata_list in index.items():
            for metadata in metadata_list:
                file_path = os.path.join(args.file_dir, year, agency, f"{doc_id}.txt")
                if not os.path.exists(file_path):
                    continue
                try:
                    with open(file_path, "r") as f:
                        text = f.read()
                except UnicodeDecodeError:
                    try:
                        with open(file_path, "r", encoding="windows-1252") as f:
                            text = f.read()
                    except UnicodeDecodeError:
                        continue

                url = None
                for file_metadata in metadata["Content Files"]:
                    if file_metadata["File Type"] in [".htm", ".txt", ".doc", ".docx"]:
                        url = file_metadata["URL"]

                record = {
                    "id": doc_id,
                    "document_type": metadata.get("Document Type"),
                    "posted_date": metadata.get("Posted Date"),
                    "title": metadata.get("Title"),
                    "text": text,
                    "agency": agency,
                    "added": datetime.datetime.utcnow().isoformat(),
                    "source": SOURCE_NAME,
                    "metadata": {"license": str(PermissiveLicenses.PD), "url": url},
                }

                yield record


def main(args):
    to_dolma(generate_records(args), args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("usgpo")
    main(args)
