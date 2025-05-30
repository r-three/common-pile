"""Preprocess the ubuntu data."""

import argparse
import glob
import multiprocessing as mp
import os
import re
from tempfile import TemporaryDirectory

from common_pile.logs import configure_logging
from common_pile.write import ShardParallelProcessor

# By configuring the logger as a module level logger with the name
# dolma.ProcessorClassName, our logger configuration is used (although dolma
# forces a WARN level). The other option is to override `cls.get_logger` to
# set it up yourself (allowing you to do things like use a lower log level.)
configure_logging("dolma.UbuntuChatParallel")

parser = argparse.ArgumentParser(
    description="Preprocess raw chats in the dolma format."
)
parser.add_argument(
    "--input",
    default="data/ubuntu-chat/raw",
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    default="data/ubuntu-chat/v0",
    help="The output version, this directory should be where the `documents` dir will live.",
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
# This is currently unused, if we filter out chats in the dolma format we will
# have unbalanced shards. We should have an efficient re-shard tool before we
# apply this filter.
parser.add_argument(
    "--min_lines",
    default=5,
    type=int,
    help="The number of lines a cleaned chat needs to be included. (Currently un-used.)",
)

BOT_NAMES = (
    "<ubottu>",
    "<ubuntulog>",
    "<ubuntulog2>",
    "<ubot5>",
    "<ubot93>",
    "<ukbot>",
    "<uvirtbot>",
    "<Meetingology>",
    "<queuebot>",
    "<twobottux>",
    "<AirBot>",
    "<IrcsomeBot>",
    "<lubot>",
    "<uBOTu-fr>",
    "<ubotu-search>",
    "<Fibubot>",
    "<CyberKing>",
    "<lubotu1>",
    "<lubotu2>",
    "<lubotu3>",
    "<kubot>",
    "<IRSeekBot>",
    "<IRCAnswersBot>",
)


class UbuntuChatParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, /, min_lines: int = 5, **kwargs):
        text = example["text"]
        # Remove Channel Announcements
        text = re.sub(r"===.*\n", "", text)
        # Remove Telegram Bot Names
        text = re.sub(
            r"(\[[0-9][0-9]:[0-9][0-9]\] )<[^\s]*> (\[telegram\] )?(<[^\s]*>):?",
            "\1\3",
            text,
        )
        # Remove Bots
        text = re.sub(
            rf"\[[0-9][0-9]:[0-9][0-9]\] ({'|'.join(BOT_NAMES)}) .*\n?",
            "",
            text,
        )
        text = text.strip()
        # TODO: Check for min lines
        if not text:
            return None
        # Extract authors and action authors.
        # Look at the start of the line to avoid picking up authors that are quoted.
        authors = set(
            re.findall(r"^\[[0-9][0-9]:[0-9][0-9]\] <(\S*?)>", text, re.MULTILINE)
        )
        action_authors = set(
            re.findall(r"^\[[0-9][0-9]:[0-9][0-9]\]  \* (\S*)", text, re.MULTILINE)
        )
        authors = sorted(authors | action_authors)
        # Update the example.
        example["metadata"]["authors"] = authors
        example["text"] = text
        return example


def main(args):
    with TemporaryDirectory() as tempdir:
        processors = UbuntuChatParallel(
            source_prefix=os.path.join(args.input, "documents", "*_ubuntu.jsonl.gz"),
            destination_prefix=os.path.join(args.output, "documents"),
            metadata_prefix=tempdir,
            num_processes=args.processes,
        )
        processors(debug=args.debug, min_lines=args.min_lines)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
