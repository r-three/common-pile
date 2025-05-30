"""Preprocess Python PEPs."""

import argparse
import multiprocessing as mp
import re
from datetime import datetime

import pypandoc

from common_pile import logs, utils
from common_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(description="Preprocess raw peps in the dolma format.")
parser.add_argument(
    "--input",
    default="data/peps-dolma/raw",
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    default="data/peps-dolma/v0",
    help="The output version, this directory should be where the `documents` dir will live.",
)
parser.add_argument(
    "--debug",
    action="store_true",
    help="Should we log when documents are not changed by preprocessing.",
)
parser.add_argument(
    "--processes",
    type=int,
    default=mp.cpu_count(),
    help="Number of processors for multicore.",
)
parser.add_argument(
    "--meta",
    help="Location to store Dolma Metadata information.",
)


logs.configure_logging()


def extract_created(text):
    if m := re.search(r"^Created: (?P<date>.*)$", text, re.MULTILINE):
        return m.group("date").strip()


def parse_date(date):
    return datetime.strptime(date, "%d-%b-%Y")


def extract_authors(text):
    if m := re.search(
        r"^Author: (?P<authors>.*?)^.*?:", text, re.MULTILINE | re.DOTALL
    ):
        return m.group("authors")


def parse_authors(authors):
    authors = re.sub(r"<.*?>", "", authors)
    authors = authors.split(",")
    return sorted([a_ for a in authors if (a_ := a.strip())])


def process_pep(text):
    return re.sub(r":pep:`(\d{1,4})`", r"PEP \1", text)


def clean_rst(text):
    return pypandoc.convert_text(text, "plain", format="rst").strip()


class PEPParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        logger = cls.get_logger()

        with logger(id=example["id"]):
            logger.debug("Processing example")
            pep = example["text"]

            created = extract_created(pep)
            created = parse_date(created)
            example["created"] = created.isoformat()

            authors = extract_authors(pep)
            example["metadata"]["authors"] = parse_authors(authors)

            # Update this if the implementation of clean_rst changes.
            example["metadata"]["pandoc_version"] = pypandoc.get_pandoc_version()

            pep = process_pep(pep)
            example["text"] = clean_rst(pep)
            return example


def main(args):
    with utils.maybe_temp_dir(path=args.meta) as meta_dir:
        processor = PEPParallel(
            source_prefix=utils.dolma_input(args.input, "*.jsonl.gz"),
            destination_prefix=utils.dolma_output(args.output),
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor(debug=args.debug)


if __name__ == "__main__":
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
