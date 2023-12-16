"""Preprocess the arxiv data."""

import argparse
import glob
import multiprocessing as mp
import os
from tempfile import TemporaryDirectory

from licensed_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(
    description="Preprocess raw arxiv in the dolma format."
)
parser.add_argument(
    "--input",
    default="data/ubuntu-chat/raw",
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    default="data/ubuntu-chat/v0",
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


class ArxivParallel(ShardParallelProcessor):
    @classmethod
    def precess_example(cls, example, **kwargs):
        return example


def main(args):
    with TemporaryDirectory() as tempdir:
        processors = ArxivParallel(
            source_prefix=os.path.join(args.input, "documents", "*_arxiv.jsonl.gz"),
            destination_prefix=os.path.join(args.output, "documents"),
            metadata_prefix=tempdir,
            num_processes=args.processes,
        )
        processors(debug=args.debug)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
