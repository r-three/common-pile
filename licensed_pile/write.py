"""Utilities that have to do with writing data."""

import abc
import copy
import datetime
import json
import logging
import multiprocessing as mp
import os
from contextlib import ExitStack
from queue import Queue
from typing import Dict, Iterator

import contextual_logger
import smart_open
import tqdm
from dolma.core.parallel import BaseParallelProcessor

from licensed_pile.logs import configure_logging, get_logger


def shard_name(filename: str, shard: str, padding: int = 5):
    """Convert a shard count into a name with leading zeros for easy sorting."""
    return f"{shard:>0{padding}}_{filename}"


def serialize_datetime(obj):
    """Convert datetime.datetime to ISO format string for JSON serialization."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    else:
        raise ValueError(f"Object of type {type(obj)} is not serializable.")


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
            data = json.dumps(example, default=serialize_datetime)
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


def smart_open_exists(path):
    try:
        with smart_open.open(path):
            return True
    except:
        return False


def create_shadow(path):
    h, t = os.path.split(path)
    # Add shadow at the start to not break any filename inference from smart_open
    return os.path.join(h, f"shadow.{t}")


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
    def get_logger(cls):
        return get_logger()

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
            logger.debug("Processing %s into %s", source_path, destination_path)
            if not overwrite and smart_open_exists(destination_path):
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

                            og = copy.deepcopy(data["text"]) if debug else None
                            processed = cls.process_example(
                                data, source_file=source_path, line_number=i, **kwargs
                            )
                            if processed is None:
                                logger.warning(
                                    "Preprocessing has reduced example to nothing, skipping"
                                )
                                document_count += 1
                                continue

                            if debug and og == processed["text"]:
                                logger.warning("Text unchanged for example.")

                            wf.write(json.dumps(processed) + "\n")
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
