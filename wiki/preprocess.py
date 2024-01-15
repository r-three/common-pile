#!/usr/bin/env python3

import argparse
import glob
import multiprocessing as mp
import os
import re
from tempfile import TemporaryDirectory

import requests
import tqdm

from licensed_pile import logs
from licensed_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(description="Preprocess raw books in dolma format.")
parser.add_argument(
    "--input",
    default="dump/data/wiki/dump/raw",
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    default="dump/data/wiki/dump/v0",
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

logs.configure_logging("dolma.WTFWikipediaParallel")


def parse_wikitext(text, doc_id, source):
    return requests.post(
        "http://localhost:3000", json={"wikitext": text, "id": doc_id, "source": source}
    ).json()["text"]


class WTFWikipediaParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        example["text"] = parse_wikitext(
            example["text"], example["id"], example["source"]
        )
        return example


def main(args):
    with TemporaryDirectory() as tempdir:
        processor = WTFWikipediaParallel(
            source_prefix=os.path.join(
                args.input, "documents", "*_wiktionary.com.jsonl.gz"
            ),
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
