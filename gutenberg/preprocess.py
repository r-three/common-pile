"""Preprocess books.

We aim to have light preprocessing so we just remove the headers and footers.

Many of the books start with a small paragraph about who it was produced by. We
currently don't filter this out as it is vary variable.
"""

import argparse
import glob
import multiprocessing as mp
import os
import re
from tempfile import TemporaryDirectory

import tqdm

from common_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(description="Preprocess raw books in dolma format.")
parser.add_argument(
    "--input",
    default="data/project-gutenberg/raw",
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    default="data/project-gutenberg/v0",
    help="The output version, this directory should be where the `documents` dir will live.",
)
# TODO: Respect this flag
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously processed examples?",
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


HEADER = re.compile(
    r"(?:\*\*\* ?)? ?START OF (THE|THIS) PROJECT GUTENBERG.*?(?:\*\*\*)?$",
    re.MULTILINE | re.IGNORECASE,
)
FOOTER = re.compile(
    r"(?:\*\*\* ?)? ?END OF (THE|THIS) PROJECT GUTENBERG.*?(?:\*\*\*)?",
    re.MULTILINE | re.IGNORECASE,
)


def strip_header(text: str, header=HEADER) -> str:
    if m := header.search(text):
        text = text[m.end() :]
    return text


def strip_footer(text: str, footer=FOOTER) -> str:
    if m := footer.search(text):
        text = text[: m.start()]
    return text


class ProjectGutenbergParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        example["text"] = strip_footer(strip_header(example["text"])).strip()
        return example


def main(args):
    with TemporaryDirectory() as tempdir:
        processor = ProjectGutenbergParallel(
            source_prefix=os.path.join(args.input, "documents", "*_pg.jsonl.gz"),
            destination_prefix=os.path.join(args.output, "documents"),
            metadata_prefix=tempdir,
            num_processes=args.processes,
        )
        processor(debug=args.debug)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
