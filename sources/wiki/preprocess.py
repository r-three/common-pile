#!/usr/bin/env python3

import argparse
import glob
import multiprocessing as mp
import os
import re
from tempfile import TemporaryDirectory

import requests
import tqdm

import wiki
from common_pile import logs, utils
from common_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(description="Preprocess raw wikitext in dolma format.")
parser.add_argument(
    "--input",
    default="dump/data/wiki/dump/raw",
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    default="dump/data/wiki/dump/v0",
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
parser.add_argument(
    "--meta",
    help="Location to store Dolma Metadata information.",
)
parser.add_argument(
    "--no_shadow",
    action="store_false",
    help="Disable shadow paging, for things like cloud storage.",
)

logs.configure_logging(level="INFO")


# These are pages that often crashed the servers.
DENYLIST = {
    "Template:Attached KML/U.S. Route 62 in Kentucky",
    "Template:Attached KML/U.S. Route 277",
    "User:BeywheelzLetItRip/fonts.css",
    "User:BeywheelzLetItRip/fonts2.cs",
    "Template:Graph:Map/Inner/USA-json",
}


class WTFWikipediaParallel(ShardParallelProcessor):
    @classmethod
    def parse_wikitext(cls, wikitext, ex_id, ex_src):
        logger = cls.get_logger()
        try:
            return wiki.parse_wikitext(wikitext, ex_id, ex_src)
        except requests.Timeout:
            logger.error("Wikitext parsing: timed out")
            # Returning None for the whole example will filter it from the output.
            return None
        except (ValueError, requests.JSONDecodeError):
            logger.error(
                "Failed wikitext parsing for example",
                exc_info=True,
            )
            # Returning None for the whole example will filter it from the output.
            return None
        except Exception as e:
            e.add_note(f"Failed to parse wikitext for example: {ex_src}/{ex_id}")
            logger.error("Failed to parse wikitext for example")
            raise

    @classmethod
    def process_example(cls, example, **kwargs):
        logger = cls.get_logger()
        with logger(source=example["source"], id=example["id"]):
            if (title := example["metadata"]["title"]) in DENYLIST:
                logger.warning(
                    "Skipping example from deny list as the text is %d characters long.",
                    len(example["text"]),
                    extra={"title": title},
                )
                # Returning None for the whole example will filter it from the output.
                return None
            wikitext = example["text"]
            # TODO: The dolma generation script should not include empty text.
            if not wikitext:
                logger.warning("Example is empty, skipping")
                # Returning None for the whole example will filter it from the output.
                return None
            # Eventually add length filtering?
            # if len(wikitext) > 1_000_000:
            #     logger.warning("Skipping example as the text is %d characters long.", len(wikitext))
            #     return None
            # Convert <math> -> $$...$$
            wikitext = wiki.replace_math_tags(wikitext)
            # Adjust indentation to avoid reorderings.
            wikitext = wiki.adjust_indentation(wikitext)
            # Extract Templates
            wikitext, math_templates = wiki.extract_templates(
                wikitext, ("math",), wiki.MATH_MARKER
            )
            if math_templates:
                logger.debug("Found %d {{math|...}} templates.", len(math_templates))
            wikitext, raw_templates = wiki.extract_templates(
                wikitext, wiki.MATH_TEMPLATES, wiki.SECOND_MARKER
            )
            if raw_templates:
                logger.debug(
                    "Found %d more templates that appear to contain math.",
                    len(raw_templates),
                )

            # We replace these symbols after extracting any thare are part of other
            # templates. Trying to extract these as their own templates (optional \)
            # creates weird issues like {{Infobox ...}} getting extracted as {{In..}}
            wikitext = wiki.replace_symbols(wikitext, include_money=True)

            # Parse Wiki Text
            document = cls.parse_wikitext(wikitext, example["id"], example["source"])
            # TODO: Remove the double checking for document being empty
            if document is None:
                logger.warning(
                    "Wikitext parsing reduced example to nothing.",
                )
                # Returning None for the whole example will filter it from the output.
                return None

            # Format plaintext into document
            document = wiki.format_document(
                document, example.get("metadata", {}).get("title", "")
            )
            if not document:
                logger.warning(
                    "Wikitext parsing reduced example to nothing.",
                )
                # Returning None for the whole example will filter it from the output.
                return None

            # Process Templates
            math_templates = map(wiki.fix_math, math_templates)
            parsed_templates = [
                cls.parse_wikitext(t, example["id"], example["source"])
                for t in math_templates
            ]
            parsed_templates = [
                p[0]["text"] if p is not None else "" for p in parsed_templates
            ]
            for mt, pt in zip(math_templates, parsed_templates):
                if not pt:
                    logger.warning(
                        "Math template `%s` was parsed to nothing.",
                        mt,
                    )

            parsed_templates = [
                t.replace(wiki.ABS_MARKER, "|") for t in parsed_templates
            ]
            parsed_templates = [f"${t}$" for t in parsed_templates]

            raw_templates = map(wiki.fix_math, raw_templates)
            parsed_raw = [
                cls.parse_wikitext(t, example["id"], example["source"])
                for t in raw_templates
            ]
            parsed_raw = [p[0]["text"] if p is not None else "" for p in parsed_raw]
            for rt, pr in zip(raw_templates, parsed_templates):
                if not pr:
                    logger.warning(
                        "Template `%s` was parsed to nothing.",
                        rt,
                    )
            parsed_raw = [t.replace(wiki.ABS_MARKER, "|") for t in parsed_raw]
            parsed_raw = [f"${t}$" for t in parsed_raw]
            # Reinsert Templates
            document = wiki.insert_templates(document, parsed_raw, wiki.SECOND_MARKER)
            document = wiki.insert_templates(
                document, parsed_templates, wiki.MATH_MARKER
            )
            example["text"] = document
            return example


def main(args):
    with utils.maybe_temp_dir(path=args.meta) as meta_dir:
        processor = WTFWikipediaParallel(
            source_prefix=utils.dolma_input(args.input, args.filename),
            destination_prefix=utils.dolma_output(args.output),
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor(debug=args.debug, overwrite=args.overwrite, shadow=not args.no_shadow)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
