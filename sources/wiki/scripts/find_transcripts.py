"""Find Transcripts in domains, urls, and titles."""

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
    description="Find documents that are most likely licensed laundered transcripts."
)
parser.add_argument(
    "--input",
    required=True,
    help="The path to the dolma data we are looking for transcripts in.",
)
parser.add_argument(
    "--output",
    default="transcripts.jsonl",
    help="Where to save the documents that appear to be transcripts for look for false positives.",
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
    help="Should we not include special case transcripts? (Don't include known false positives)",
)


def read_shard(path):
    with smart_open.open(path) as f:
        yield from (json.loads(l) for l in f if l)


def has_transcript(s, transcript: str = "transcript") -> bool:
    if s is None:
        return False
    return transcript in s.lower()


def special_case(example):
    # Copied from the filter_transcripts script. This is just used
    # to get a quick check of how many special cases (false
    # positives) are saved from the chopping block, so perfect
    # reuse isn't super important.

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


def find_transcript(example, save_special: bool = False):
    logger = logs.get_logger()
    if (
        has_transcript(example["metadata"].get("url", ""))
        or has_transcript(example["metadata"].get("dumpurl", ""))
        or has_transcript(example["metadata"].get("title", ""))
        or has_transcript(example.get("source", ""))
    ):
        # Consider removing the text if there are too many examples/they are too big
        # Leave it for now to see if there are exceptions to keep (transcripts of calls etc).
        # example.pop("text")
        logger.info(
            "Found transcript.",
            extra={
                "source": example["source"],
                "title": example["metadata"].get("title", ""),
            },
        )
        if save_special and special_case(example):
            logger.info(
                "Keeping transcript as it is a special case.",
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
    find_scripts = functools.partial(find_transcript, save_special=save_special)
    return list(filter(lambda x: x, map(find_scripts, data)))


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
            f"Saving examples that have transcripts in their URL/title to {args.output}"
        )
        with open(args.output, "w") as wf:
            for example in tqdm.tqdm(examples_with_transcript):
                wf.write(json.dumps(example) + "\n")


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging()
    main(args)
