"""Filter out transcripts."""

import argparse
import multiprocessing as mp
import re

from common_pile import logs, utils
from common_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(
    description="Remove probably license laundering from dolma documents in the form of verbatim transcripts."
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


def has_transcript(s, transcript: str = "transcript") -> bool:
    # Copied from the find_transcripts script.
    if s is None:
        return False
    return transcript in s.lower()


def find_transcript(example):
    # Copied from the find_transcripts script.
    return (
        has_transcript(example["metadata"].get("url", ""))
        or has_transcript(example["metadata"].get("dumpurl", ""))
        or has_transcript(example["metadata"].get("title", ""))
        or has_transcript(example.get("source", ""))
    )


def special_case(example):
    # Had some programs that used "transcript" in them.
    if "openitware" in example["source"].lower():
        return True
    # Lots of geneology information comes from transcripts of old records."
    if (
        example["metadata"]["url"]
        and "familysearch.org" in example["metadata"]["url"].lower()
    ):
        return True
    # wiki-scratchpad has a lot of fanfic "transcriptions" but also a lot of real ones, hard to split, toss it all
    # Some wiki's keep IRC chat transcripts in their wikis
    if (
        example["metadata"]["title"]
        and "irc meeting" in example["metadata"]["title"].lower()
    ):
        return True
    # Lots of documents about DNA/RNA transcription
    if "proteopedia" in example["source"]:
        return True
    # Lots of fanfic written as transcripts.
    if "wiki-ideasfandomcom" in example["source"]:
        return True
    if (
        example["metadata"]["url"]
        and "ideas.fandom.com" in example["metadata"]["url"].lower()
    ):
        return True
    if "calvinandhobbesfanon" in example["source"]:
        return True
    if "differenthistory" in example["source"]:
        return True
    if "cartoonnetworkfanfiction" in example["source"]:
        return True
    # Lots of talk about experience with different transcription companies
    if example["source"] == "wiki-bushlawyerconz_w":
        return True
    # Lots of their magic is called "transcription seals"
    if (
        "naruto" in example["source"]
        and example["metadata"]["title"]
        and "transcription seal" in example["metadata"]["title"].lower()
    ):
        return True
    if "christians_grade_12_chemistry" in example["source"]:
        return True
    if "wiki-piratepartyca" in example["source"]:
        return True
    return False


class FilterTranscriptParallel(ShardParallelProcessor):
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
            if find_transcript(example):
                logger.info("Found what looks to be a transcript.")
                if special_case(example):
                    logger.info("Making exception for example")
                    return example
                return None
            return example


def main(args):
    with utils.maybe_temp_dir(args.meta) as meta_dir:
        processor = FilterTranscriptParallel(
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
