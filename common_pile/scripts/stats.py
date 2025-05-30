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


class SizeStatsParallel(BaseParallelProcessor):
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
        tokens: int = 0,
        bytes_utf8: int = 0,
        characters: int = 0,
    ):
        return super().increment_progressbar(
            queue,
            shards=shards,
            documents=documents,
            tokens=tokens,
            bytes=bytes_utf8,
            characters=characters,
        )

    @classmethod
    def process_single(
        cls,
        source_path: str,
        destination_path: str,
        queue: Queue,
        **kwargs,
    ):
        del destination_path
        logger = cls.get_logger()
        logger.debug("Counting Tokens from Dolma files at %s", source_path)
        with logger(file=source_path):
            with smart_open.open(source_path) as f:
                document_count = 0
                token_count = 0
                byte_count = 0
                char_count = 0
                update_interval = kwargs.pop("update_interval", 1)

                for i, line in enumerate(f):
                    with logger(line=i):
                        try:
                            try:
                                data = json.loads(line)
                            except json.JSONDecodeError:
                                logger.error(
                                    "Failed to parse JSON from `%s...`",
                                    line[:80],
                                    exc_info=True,
                                )
                                continue
                            # TODO: Dolma file generation should not be adding null lines
                            if data is None:
                                continue
                            # TODO: Make this configurable
                            if data["text"] is None:
                                document_count += 1
                                continue
                            tokens = data["text"].split()
                            document_count += 1
                            token_count += len(tokens)
                            char_count += len(data["text"])
                            # There are some sources that have invalid unicode that result
                            # in rendering errors in webpages. Thus we ignore them here.
                            # Example: https://math.stackexchange.com/a/8849
                            byte_count += len(data["text"].encode("utf-8", "ignore"))

                            if document_count % update_interval == 0:
                                cls.increment_progressbar(
                                    queue,
                                    documents=document_count,
                                    tokens=token_count,
                                    bytes_utf8=byte_count,
                                    characters=char_count,
                                )
                                if queue.qsize() >= mp.cpu_count():
                                    update_interval *= 2
                                document_count = 0
                                token_count = 0
                                char_count = 0
                                byte_count = 0
                        except Exception as e:
                            logger.error("Failed to process example", exc_info=True)
                            raise
                cls.increment_progressbar(
                    queue,
                    shards=1,
                    documents=document_count,
                    tokens=token_count,
                    bytes_utf8=byte_count,
                    characters=char_count,
                )


def main():
    mp.set_start_method("spawn")
    parser = argparse.ArgumentParser(description="Calculate Size Stats in dolma files.")
    parser.add_argument(
        "--input",
        required=True,
        help="The dolma input directory, should be where the `documents` dir lives. Can also be a specific file.",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=mp.cpu_count(),
        help="Number of processors for multicore.",
    )
    parser.add_argument(
        "--meta", help="Location to store dolma metadata while processing."
    )
    args = parser.parse_args()

    source = utils.dolma_input(args.input)

    with utils.maybe_temp_dir(path=args.meta) as meta_dir:
        processor = SizeStatsParallel(
            source_prefix=source,
            # Unused
            destination_prefix=meta_dir,
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor()


if __name__ == "__main__":
    main()
