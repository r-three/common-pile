import argparse
import datetime
import functools
import json
import os

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Collect PubMedCentral into Dolma format.")
parser.add_argument(
    "--filelist",
    default="data/permissive_filelist.txt",
    help="The path to the filelist.txt file.",
)
parser.add_argument(
    "--data_dir", default="data/md/", help="Path to the directory of markdown files."
)
parser.add_argument(
    "--author_dir", default="data/authors/", help="Where the author files go."
)
parser.add_argument(
    "--output_dir",
    default="data/pubmedcentral/",
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


def format_dolma(
    file: str,
    data_dir: str,
    source_name: str = SOURCE_NAME,
    base_url: str = "https://www.ncbi.nlm.nih.gov/pmc/articles",
):
    file, journal, accessionID, _, lic = file.split("\t")
    file = os.path.basename(file).replace("tar.gz", "md")
    with open(os.path.join(data_dir, file)) as f:
        text = f.read()

    with open(
        os.path.join(args.author_dir, f"{os.path.splitext(file)[0]}.json"),
        encoding="utf-8",
    ) as f:
        authors = json.load(f)

    return {
        "id": accessionID,
        "text": text,
        "source": source_name,
        "added": datetime.datetime.utcnow().isoformat(),
        "metadata": {
            "license": str(LICENSE_MAP[lic]),
            "url": f"{base_url}/{accessionID}/",
            "journal": journal,
            "authors": authors,
        },
    }


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)

    with open(args.filelist) as f:
        files = f.read().split("\n")

    # Remove the header
    files = files[1:]

    files = map(functools.partial(format_dolma, data_dir=args.data_dir), files)
    to_dolma(files, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
