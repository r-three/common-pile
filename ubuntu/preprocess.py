"""Preprocess the ubuntu data."""

import argparse
import glob
import multiprocessing as mp
import os
from tempfile import TemporaryDirectory

from licensed_pile.write import ShardParallelProcessor

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
parser.add_argument(
    "--min_lines",
    default=5,
    type=int,
    help="The number of lines a cleaned chat needs to be included.",
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
        # Remove Channel Announcements
        # Remove Telegram Bot Names
        # Remove Bots
        # Check for min lines
        # Should we just have it blank for now? or skip having min lines?
        # Extract authors
        authors = ...
        action_authors = ...
        authors = sorted(authors | action_authors)
        # Update the example.
        example["metadata"]["authors"] = authors
        example["text"] = text
        return example


def main(args):
    with TemporaryDirectory() as tempdir:
        processors = UbuntuParallel(
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
