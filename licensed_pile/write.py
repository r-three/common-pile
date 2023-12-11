"""Utilities that have to do with writing data."""

from contextlib import ExitStack
import logging
import os
from queue import Queue
import json

import tqdm
import smart_open
from dolma.core.parallel import BaseParallelProcessor

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.INFO)


def shard_name(filename: str, shard: str, padding: int = 5):
    return f"{shard:>0{padding}}_{filename}"


# TODO: Add overwrite protection
def to_dolma(examples: Sequence[Dict], path: str, filename: str, shard_size: int = 1):
    """Write `examples` to `path` in the dolma format with `shard_size`G shards."""
    shard_idx = 0
    size = 0
    # Gigabytes, not Gibibytes
    max_bytes = shard_size * 1000 * 1000 * 1000
    with ExitStack() as stack:
        wf = stack.enter_context(smart_open(os.path.join(path, shard_name(filename, shard_idx)), "w"))
        for example in tqdm.tqdm(examples):
            data = json.dumps(example)
            size += len(data) * d
            if size >= max_bytes:
                wf.close()
                shard_idx += 1
                wf = stack.enter_context(smart_open(os.path.join(path, shard_name(filename, shard_idx)), "w"))
            wf.write(data + "\n")


class FileParallelProcessor(BaseParallelProcessor):

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
        with smart_open.open(source_path) as f, smart_open.open(destination_path, "w") as wf:
            logger = cls.get_logger()
            document_count = 0
            update_interval = kwargs.pop("update_interval", 1)

            try:
                for i, line in enumerate(f):
                    try:
                        data = json.loads(line)
                    except json.JSONDecodeError as e:
                        logger.warning("Failed to parse %s:%s `%s...`: %s", source_path, i, line[:80], e)
                        continue

                    processed = cls.process_example(data)

                    wf.write(json.dumps(processed) + "\n")
                    document_count += 1

                    if document_count % update_interval == 0:
                        cls.increment_progressbar(queue, documents=document_count)
                        if queue.qsize() >= multiprocessing.cpu_count():
                            update_interval *= 2
                        document_count = 0
            except Exception as e:
                logger.warning("Failed to process %s: %s", source_path, e)
                return
            cls.increment_progressbar(queue, files=1, documents=documents_count)
