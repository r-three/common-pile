"""Filter out lyric pages."""

import argparse
import multiprocessing as mp
import re

from licensed_pile import logs, utils
from licensed_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(
    description="Remove probably license laundering from dolma documents in the form of verbatim lyrics."
)
parser.add_argument(
    "--input",
    required=True,
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    required=True,
    help="The output version, this directory should be where the `documents` dir will live.",
)
parser.add_argument(
    "--filename",
    default="*.jsonl.gz",
    help="The filename to match with globs, probably needs to be escaped.",
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
parser.add_argument("--meta", help="Location to save dolma processing metadata.")

logs.configure_logging()


def has_lyric(s, lyric: str = "lyric") -> bool:
    # Copied from the find_lyrics script.
    if s is None:
        return False
    return lyric in s.lower()


def find_lyric(example, word: str = "lyric"):
    # Copied from the find_lyrics script.
    return (
        has_lyric(example["metadata"].get("url", ""), word)
        or has_lyric(example["metadata"].get("dumpurl", ""), word)
        or has_lyric(example["metadata"].get("title", ""), word)
        or has_lyric(example.get("source", ""), word)
    )


def special_case(example):
    # Lots of fanfic written as transcripts.
    if "wiki-ideasfandomcom" in example["source"]:
        return True
    if (
        example["metadata"]["url"]
        and "ideas.fandom.com" in example["metadata"]["url"].lower()
    ):
        return True
    # This wiki is all fan translations of a video game which is copyrighted.
    if "thpatch" in example["source"]:
        return False
    if "vocaloidlyrics" in example["source"]:
        return False
    # Some of the filtered pages, like from the justdance wiki are all talk pages
    # about Guess the Lyrics games which are short and low quality.
    # Similarly, a lot of detected documents from the duranduran wiki are all
    # from filenames of "with lyric" videos. These are low quality.
    if find_lyric(example, "lyrical"):
        return True
    if find_lyric(example, "lyrica"):
        return True
    return False


class FilterLyricParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        logger = logs.get_logger()
        with logger(
            source=example["source"],
            id=example["id"],
            title=example["metadata"]["title"],
            url=example["metadata"]["url"],
            dump_url=example["metadata"]["dump_url"],
        ):
            if find_lyric(example):
                logger.info("Found what looks to be a lyric page.")
                if special_case(example):
                    logger.info("Making exception for example")
                    return example
                return None
            return example


def main(args):
    with utils.maybe_temp_dir(args.meta) as meta_dir:
        processor = FilterLyricParallel(
            source_prefix=utils.dolma_input(args.input, args.filename),
            destination_prefix=utils.dolma_output(args.output),
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor(debug=args.debug, overwrite=args.overwrite)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
