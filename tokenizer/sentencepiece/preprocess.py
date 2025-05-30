#!/usr/bin/env python3

import argparse
import json
import logging
import multiprocessing as mp
import os
import re
from queue import Queue

import smart_open
from dolma.core.parallel import BaseParallelProcessor

from common_pile import logs, utils

parser = argparse.ArgumentParser(
    description="Preprocess Dolma Data for SentencePiece tokenizer training."
)
parser.add_argument(
    "--input",
    required=True,
    help="The input data, this directory should be where the `documents` dir lives.",
)
parser.add_argument("--output", required=True, help="The output location.")
parser.add_argument(
    "--filename",
    default="*.json.gz",
    help="The filename to match with globs, probably needs to be escaped.",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously processed examples?",
)
parser.add_argument(
    "--processes",
    type=int,
    default=mp.cpu_count(),
    help="Number of processors for multicore.",
)
parser.add_argument("--meta", help="Location to save dolma processing metadata.")

logs.configure_logging(level="DEBUG")


def create_shadow(path):
    h, t = os.path.split(path)
    # Add shadow at the start to not break any filename inference from smart_open
    return os.path.join(h, f"shadow.{t}")


class SentencePieceProcessor(BaseParallelProcessor):
    @classmethod
    def increment_progressbar(
        cls,
        queue: Queue,
        /,
        shards: int = 0,
        documents: int = 0,
    ):
        return super().increment_progressbar(queue, shards=shards, documents=documents)

    @classmethod
    def get_logger(cls):
        return logs.get_logger()

    @classmethod
    def process_single(
        cls,
        source_path: str,
        destination_path: str,
        queue: Queue,
        **kwargs,
    ):
        logger = cls.get_logger()
        overwrite = kwargs.pop("overwrite", False)
        shadow = kwargs.pop("shadow", True)
        with logger(file=source_path):
            destination_path = re.sub(r"\.jsonl?.gz$", ".txt", destination_path)
            logger.debug("Processing %s into %s", source_path, destination_path)
            if not overwrite and os.path.exists(destination_path):
                logger.info("%s already exists, skipping", destination_path)
                cls.increment_progressbar(queue, shards=1)
                return
            output_path = (
                create_shadow(destination_path) if shadow else destination_path
            )
            with smart_open.open(source_path) as f, smart_open.open(
                output_path, "w"
            ) as wf:
                document_count = 0
                update_interval = kwargs.pop("update_interval", 1)
                debug = kwargs.pop("debug", False)

                for i, line in enumerate(f):
                    with logger(line=i):
                        try:
                            try:
                                data = json.loads(line)
                            except json.JSONDecodeError as e:
                                logger.warning(
                                    "Failed to parse JSON from `%s...`",
                                    line[:80],
                                    exc_info=True,
                                )
                                continue

                            processed = re.sub(r"\n", "<n>", data["text"])

                            if processed is None:
                                logger.warning(
                                    "Preprocessing has reduced example to nothing, skipping"
                                )
                                document_count += 1
                                continue

                            wf.write(processed + "\n")
                            document_count += 1

                            if document_count % update_interval == 0:
                                cls.increment_progressbar(
                                    queue, documents=document_count
                                )
                                if queue.qsize() >= mp.cpu_count():
                                    update_interval *= 2
                                document_count = 0
                        except Exception as e:
                            e.add_note(
                                f"Exception occured while processing {source_path}:{i}"
                            )
                            logger.warning(
                                "Exception occured while processing example",
                                exc_info=True,
                            )
                            raise
                # Cloud Storage generally doesn't have a cheap way to rename files. So
                # shadow paging should generally only be used for local data.
                if shadow:
                    os.rename(output_path, destination_path)
                cls.increment_progressbar(queue, shards=1, documents=document_count)


def main(args):
    with utils.maybe_temp_dir(args.meta) as meta_dir:
        processor = SentencePieceProcessor(
            source_prefix=utils.dolma_input(args.input, args.filename),
            destination_prefix=utils.dolma_output(args.output),
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor(overwrite=args.overwrite)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
