"""Find Lyrics in domains, urls, and titles."""

import argparse
import functools
import glob
import itertools
import json
import multiprocessing as mp

import smart_open
import tqdm

from common_pile import logs, utils

parser = argparse.ArgumentParser(
    description="Find documents that are most likely licensed laundered lyrics."
)
parser.add_argument(
    "--input",
    required=True,
    help="The path to the dolma data we are looking for lyrics in.",
)
parser.add_argument(
    "--output",
    default="lyrics.jsonl",
    help="Where to save the documents that appear to be lyrics for look for false positives.",
)
parser.add_argument(
    "--processors",
    default=mp.cpu_count(),
    type=int,
    help="The number of processors to use when searching the shards in parallel.",
)
parser.add_argument(
    "--special_case",
    action="store_true",
    help="Should we not include special case lyrics? (Don't include known false positives)",
)


def read_shard(path):
    with smart_open.open(path) as f:
        yield from (json.loads(l) for l in f if l)


def has_lyric(s, lyric: str = "lyric") -> bool:
    if s is None:
        return False
    return lyric in s.lower()


def special_case(example):
    return False


def find_lyric(example, save_special: bool = False):
    logger = logs.get_logger()
    if (
        has_lyric(example["metadata"].get("url", ""))
        or has_lyric(example["metadata"].get("dumpurl", ""))
        or has_lyric(example["metadata"].get("title", ""))
        or has_lyric(example.get("source", ""))
    ):
        # Consider removing the text if there are too many examples/they are too big
        # Leave it for now to see if there are exceptions to keep (transcripts of calls etc).
        # example.pop("text")
        logger.info(
            "Found lyrics.",
            extra={
                "source": example["source"],
                "title": example["metadata"].get("title", ""),
            },
        )
        if save_special and special_case(example):
            logger.info(
                "Keeping lyrics as it is a special case.",
                extra={
                    "source": example["source"],
                    "title": example["metadata"].get("title", ""),
                },
            )
            return None
        return example
    return None


def process_shard(path, save_special: bool = False):
    data = read_shard(path)
    find = functools.partial(find_lyric, save_special=save_special)
    return list(filter(lambda x: x, map(find, data)))


def main(args):
    shards = utils.dolma_input(args.input)
    shards = glob.glob(shards, recursive=True)
    logger = logs.get_logger()
    logger.info(f"Found {len(shards)} shards to check.")

    process = functools.partial(process_shard, save_special=args.special_case)

    # Manually parallelize the shard processing as dolma parallel
    # processing isn't setup for collecting the results at the end.
    with mp.Pool(args.processors) as pool:
        examples_with_transcript = itertools.chain(*pool.imap(process, shards))

        logger.info(
            f"Saving examples that have lyrics in their URL/title to {args.output}"
        )
        with open(args.output, "w") as wf:
            for example in tqdm.tqdm(examples_with_transcript):
                wf.write(json.dumps(example) + "\n")


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging()
    main(args)
