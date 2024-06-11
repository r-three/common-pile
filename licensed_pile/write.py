"""Utilities that have to do with writing data."""

import abc
import copy
import json
import multiprocessing as mp
import os
from contextlib import ExitStack
from queue import Queue
from typing import Dict, Iterator

import smart_open
import tqdm
from dolma.core.parallel import BaseParallelProcessor

from licensed_pile.logs import configure_logging, get_logger


def shard_name(filename: str, shard: str, padding: int = 5):
    """Convert a shard count into a name with leading zeros for easy sorting."""
    return f"{shard:>0{padding}}_{filename}"


# TODO: Add overwrite protection
def to_dolma(
    examples: Iterator[Dict],
    path: str,
    filename: str,
    shard_size: int = 1,
    quiet: bool = False,
):
    """Write `examples` to `path` in the dolma format with `shard_size`GB shards."""
    logger = get_logger()
    logger.info("Writing Dolma Shards to %s", path)
    os.makedirs(path, exist_ok=True)
    shard_idx = 0
    size = 0
    # Gigabytes, not Gibibytes
    max_bytes = shard_size * 1000 * 1000 * 1000
    with ExitStack() as stack:
        wf = stack.enter_context(
            smart_open.open(os.path.join(path, shard_name(filename, shard_idx)), "w")
        )
        for example in tqdm.tqdm(examples, disable=quiet):
            data = json.dumps(example)
            # Assume one character is about 1 bytes, good enough as we use utf-8
            size += len(data)
            if size >= max_bytes:
                wf.close()
                shard_idx += 1
                shard_file = os.path.join(path, shard_name(filename, shard_idx))
                wf = stack.enter_context(smart_open.open(shard_file, "w"))
                logger.info("Shard size exceeded, creating new shard at %s", shard_file)
                size = 0
            wf.write(data + "\n")


class ShardParallelProcessor(BaseParallelProcessor):
    """Handle read/writes to jsonl.gz so our processor code only needs to processing a single example."""

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
    @abc.abstractmethod
    def process_example(cls, example, **kwargs):
        """Code to process a single example in the dolma format, not the whole file."""

    @classmethod
    def process_single(
        cls,
        source_path: str,
        destination_path: str,
        queue: Queue,
        **kwargs,
    ):
        logger = cls.get_logger()
        logger.debug("Processing %s into %s", source_path, destination_path)
        with smart_open.open(source_path) as f, smart_open.open(
            destination_path, "w"
        ) as wf:
            document_count = 0
            update_interval = kwargs.pop("update_interval", 1)
            debug = kwargs.pop("debug", False)

            try:
                for i, line in enumerate(f):
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning(
                            "Failed to parse %s:%s `%s...`: %s",
                            source_path,
                            i,
                            line[:80],
                            e,
                        )
                        continue

                    if debug:
                        og = copy.deepcopy(data["text"])

                    processed = cls.process_example(data, **kwargs)

                    if processed is None:
                        logger.warning(
                            "Preprocessing has reduced %s:%s to nothing, skipping",
                            source_path,
                            i,
                        )
                        continue

                    if debug and og == processed["text"]:
                        logger.warning(
                            "Text unchanged for example %s:%s", source_path, i
                        )

                    wf.write(json.dumps(processed) + "\n")
                    document_count += 1

                    if document_count % update_interval == 0:
                        cls.increment_progressbar(queue, documents=document_count)
                        if queue.qsize() >= mp.cpu_count():
                            update_interval *= 2
                        document_count = 0
            except Exception as e:
                logger.warning("Failed to process %s: %s", source_path, e)
                return
            cls.increment_progressbar(queue, shards=1, documents=document_count)
