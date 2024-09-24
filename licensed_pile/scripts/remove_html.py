#!/usr/bin/env python3

import argparse
import multiprocessing as mp
import re
from tempfile import TemporaryDirectory

import bs4

from licensed_pile import logs, utils
from licensed_pile.write import ShardParallelProcessor

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

logs.configure_logging(level="DEBUG")


class CaptureMatches:
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
        cm = CaptureMatches()
        # Capture the smallest amount of text between <div or <font and >
        # This would not be ok if we cared about malicious input.
        # cleaned_text = re.sub(r"(<(?:div|font).*?>)", cm, example["text"])
        cleaned_text = re.sub(r"(<[^ >][^>]*?>)", cm, example["text"])

        if cm:
            for m in cm:
                logger.debug(
                    "Removed %s based on regex",
                    m,
                    extra={
                        "source": example["source"],
                        "example_id": example["id"],
                        "match": m,
                    },
                )

        example["text"] = cleaned_text
        return example


class BS4RemoveHTMLParallel(ShardParallelProcessor):
    """There are issues with using bs4 to remove partial html."""

    @classmethod
    def process_example(cls, example, **kwargs):
        logger = cls.get_logger()
        try:
            example["text"] = bs4.BeautifulSoup(
                example["text"], "html.parser"
            ).get_text()
        except bs4.ParserRejectedMarkup:
            # If this exception is raised, it will be before the assignment so
            # example["text"] is still the original text.
            logger.warning(
                "Failed to remove HTML, probably due to text that looks likes an html tag, keeping text as is.",
                extra={
                    "source": example["source"],
                    "example_id": example["id"],
                },
                exc_info=True,
            )
        except:
            logger.error(
                "Failed to parse HTML",
                extra={
                    "source": example["source"],
                    "example_id": example["id"],
                },
                exc_info=True,
            )
            # Just pass the text through for now
        return example


def main(args):
    with TemporaryDirectory() as tempdir:
        processor = RegexRemoveHTMLParallel(
            source_prefix=utils.dolma_input(args.input, args.filename),
            destination_prefix=utils.dolma_output(args.output),
            metadata_prefix=tempdir,
            num_processes=args.processes,
        )
        processor(debug=args.debug, overwrite=args.overwrite)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
