#!/usr/bin/env python3

import argparse
import multiprocessing as mp
import re
from tempfile import TemporaryDirectory

import bs4

from common_pile import logs, utils
from common_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(description="Remove HTML from dolma documents.")
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

logs.configure_logging(level="DEBUG")


class CaptureMatches:
    """A class that records what matches were found when doing re.serach"""

    def __init__(self):
        self.matches = []

    def __call__(self, m):
        try:
            self.matches.append(m.group(1))
        except IndexError:
            self.matches.append(m)
        return ""

    def __iter__(self):
        yield from self.matches

    def __bool__(self):
        return bool(self.matches)


class RegexRemoveHTMLParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        logger = cls.get_logger()
        with logger(source=example["source"], example_id=example["id"]):
            cm = CaptureMatches()
            # Capture the smallest amount of text between <div or <font and >
            # This would not be ok if we cared about malicious input.
            # cleaned_text = re.sub(r"(<(?:div|font).*?>)", cm, example["text"])
            # cleaned_text = re.sub(r"(<[^ >][^>]*?>)", cm, example["text"])
            # Looking at matches found the long ones tended to be false positives.
            cleaned_text = re.sub(
                r"(<(?:[a-zA-Z/]|\\\\/|\?xml|\?php|!--)[^>]{0,500}>)",
                cm,
                example["text"],
                re.DOTALL,
            )

            # If we found a </${tag}>, make sure that we didn't miss the
            # <${tag} ${attrs}> version in the text. This is useful for things
            # like tags in other languages that I noticed.
            backtracking = set()
            if cm:
                for m in cm:
                    logger.debug("Removed %s based on regex", m, extra={"match": m})
                    # Some of the ones I found have trailing spaces, but I don't want
                    # to grab something that has a bunch of attributes
                    if m := re.search(r"^</(\S+) ?>$", m):
                        # We grab the group so the sapce is removed.
                        backtracking.add(re.escape(m.group(1)))

            if backtracking:
                cm = CaptureMatches()
                backtracking = rf"(<(?:{'|'.join(backtracking)} ?.*?)>)"
                cleaned_text = re.sub(backtracking, cm, cleaned_text, re.DOTALL)
                if cm:
                    for m in cm:
                        logger.debug(
                            "Removed %s based on backtracking regex",
                            m,
                            extra={"match": m},
                        )

        example["text"] = cleaned_text
        return example


def main(args):
    with utils.maybe_temp_dir(args.meta) as meta_dir:
        processor = RegexRemoveHTMLParallel(
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
