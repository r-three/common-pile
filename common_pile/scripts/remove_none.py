"""Count the number of (whitespace-delineated) tokens in a dolma dataset."""

import argparse
import json
import multiprocessing as mp
import os
import re
from queue import Queue
from tempfile import TemporaryDirectory

import smart_open
from dolma.core.parallel import BaseParallelProcessor

from common_pile import utils
from common_pile.logs import configure_logging, get_logger

configure_logging()


class RemoveNoneParallel(BaseParallelProcessor):
    @classmethod
    def get_logger(cls):
        return get_logger()

    @classmethod
    def increment_progressbar(
        cls,
        queue: Queue,
        /,
        shards: int = 0,
        documents: int = 0,
        nones: int = 0,
    ):
        return super().increment_progressbar(
            queue,
            shards=shards,
            documents=documents,
            nones=nones,
        )

    @classmethod
    def process_single(
        cls,
        source_path: str,
        destination_path: str,
        queue: Queue,
        **kwargs,
    ):
        logger = cls.get_logger()
        with logger(file=source_path):
            logger.debug("Removing None's from Dolma files at %s", source_path)
            with smart_open.open(source_path) as f, smart_open.open(
                destination_path, "w"
            ) as wf:
                document_count = 0
                none_count = 0
                update_interval = kwargs.pop("update_interval", 1)

                for i, line in enumerate(f):
                    with logger(line=i):
                        try:
                            try:
                                data = json.loads(line)
                            except json.JSONDecodeError as e:
                                logger.error(
                                    "Failed to parse JSON from `%s...`",
                                    line[:80],
                                    exc_info=True,
                                )
                                continue

                            document_count += 1
                            if data is None:
                                none_count += 1
                            else:
                                wf.write(json.dumps(data) + "\n")

                            if document_count % update_interval == 0:
                                cls.increment_progressbar(
                                    queue,
                                    documents=document_count,
                                    nones=none_count,
                                )
                                if queue.qsize() >= mp.cpu_count():
                                    update_interval *= 2
                                document_count = 0
                                none_count = 0
                        except Exception:
                            logger.error(
                                "Failed to process example", source_path, exc_info=True
                            )
                            raise
                cls.increment_progressbar(
                    queue, shards=1, documents=document_count, nones=none_count
                )


def main():
    mp.set_start_method("spawn")
    parser = argparse.ArgumentParser(description="Remove None's from dolma files.")
    parser.add_argument(
        "--input",
        required=True,
        help="The dolma input directory, should be where the `documents` dir lives. Can also be a specific file.",
    )
    parser.add_argument("--output", required=True, help="The dolma output directory")
    parser.add_argument(
        "--processes",
        type=int,
        default=mp.cpu_count(),
        help="Number of processors for multicore.",
    )
    parser.add_argument("--meta", help="Location of Dolma processing metadata.")
    args = parser.parse_args()

    source = utils.dolma_input(args.input)
    destination = utils.dolma_output(args.output)

    with utils.maybe_temp_dir(args.meta) as meta_dir:
        processor = RemoveNoneParallel(
            source_prefix=source,
            destination_prefix=destination,
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor()


if __name__ == "__main__":
    main()
