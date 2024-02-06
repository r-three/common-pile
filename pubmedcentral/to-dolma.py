import argparse
import datetime
import functools
import os

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Collect PubMedCentral into Dolma format.")
parser.add_argument("--filelist", help="The path to the filelist.txt file.")
parser.add_argument(
    "--data_dir", default="data/raw/", help="Path to the directory of markdown files."
)
parser.add_argument(
    "--output_dir",
    default="data/dolma/",
    help="Where the dolma formatted data goes.",
)
parser.add_argument(
    "--filename",
    default="pubmedcentral.jsonl.gz",
    help="The base filename for our books.",
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)


LICENSE_MAP = {
    "CC0": PermissiveLicenses.CC0,
    "CC BY": PermissiveLicenses.CC_BY,
    "CC BY-SA": PermissiveLicenses.CC_BY_SA,
}
SOURCE_NAME = "PubMed Central"


def format_dolma(example: str, data_dir: str, source_name: str = SOURCE_NAME):
    file, citation, accessionID, PMID, lic = example.split("\t")
    file = file.split("/")[-1].replace("tar.gz", "md")
    with open(os.path.join(data_dir, file)) as f:
        text = f.read()

    return {
        "id": accessionID,
        "text": text,
        "source": source_name,
        "added": datetime.datetime.utcnow().isoformat(),
        "metadata": {
            "license": str(LICENSE_MAP[lic]),
            "url": f"https://www.ncbi.nlm.nih.gov/pmc/articles/{accessionID}/",
            "journal": citation,
        },
    }


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    with open(args.filelist) as f:
        examples = f.read().split("\n")

    # Remove the header
    examples = examples[1:]

    for example in examples:
        print(example)

    examples = map(functools.partial(format_dolma, data_dir=args.data_dir), examples)
    to_dolma(examples, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
