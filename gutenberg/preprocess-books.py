"""Preprocess books."""

import argparse
import multiprocessing as mp
import re
import glob
import os
import tqdm
import gutenbergpy.textget

from licensed_pile.write import FileParallelProcessor


parser = argparse.ArgumentParser(description="Preprocess raw books in dolma format.")
parser.add_argument("--input", default="data/project-gutenberg/raw")
parser.add_argument("--output", default="data/project-gutenberg/v0")
parser.add_argument("--overwrite", action="store_true")


class ProjectGutenbergParallel(FileParallelProcessor):

    @classmethod
    def process_example(cls, example, **kwargs):
        example["text"] = preprocess(example["text"])
        return example


def preprocess(text):
    # This library leaves license text on several books such as 71224.txt
    return gutenbergpy.textget.strip_headers(text.encode("utf-8")).decode("utf-8") + "****************** BUTTS! ***********************"


def main(args):

    with TemporaryDirectory() as tempdir:
        processor = ProjectGutenbergParallel(
            source_prefix=os.path.join(args.input, "documents/*.gz"),
            destination_prefix=os.path.join(args.output, "documents"),
            metadata_prefix=tempdir,
            num_processes=args.processes,
        )
        processor()


if __name__ == "__main__":
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
