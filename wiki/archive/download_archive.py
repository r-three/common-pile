"""Download wiki dumps from the internet archive."""

import argparse
import functools
import json
import multiprocessing.dummy as mp
import os
import random

import internetarchive
import pyunpack
import utils

from common_pile import logs

parser = argparse.ArgumentParser(
    description="Download wiki dumps from the internet archive."
)
parser = argparse.ArgumentParser("--dest_dir", default="data/archive/dumps", help="")
parser.add_argument("--wiki_metadata", default="data/archive/ia-wiki-metadata.jsonl")
parser.add_argument("--test_run", type=int, help="")
parser.add_argument("--num_threads", type=int, default=32, help="")
parser.add_argument("--worker_id", type=int, help="")
parser.add_argument("--num_workers", type=int, help="")


def download_and_extract(
    ident: str,
    dl_file,
    output_dir: str = "data/archive/dumps",
    verbose: bool = True,
):
    """Download from the IA and uncompress it."""
    logger = logs.get_logger()
    # Turn wiki id's into nested dirs for easier/faster traversal.
    dest = os.path.join(output_dir, utils.wiki_to_dir(ident))
    # Don't re-download files.
    if os.path.exists(dest):
        logger.info(
            f"Skipping download of {dl_file['name']} as {dest} already exists on disk."
        )
        return dest
    else:
        # Download using the IA tools, this includes doing a checksum to verify
        # the download was correct.
        logger.info(f"Downloading {dl_file['name']}.")
        try:
            internetarchive.download(
                ident,
                checksum=True,
                verbose=verbose,
                files=dl_file["name"],
                destdir=output_dir,
            )
        except:
            # TODO: Should we ensure the dest dir is deleted in case of failure?
            logger.error(f"Failed to download {dl_file['name']}", exc_info=True)
            try:
                os.rmdir(dest)
            except:
                logger.error(
                    f"Failed to remove {dest} after a failed download.", exc_info=True
                )
            return dest
    logger.info(f"Extracting download at {dest}.")
    try:
        # pyunpack wraps multiple extraction tools, and picks the right one.
        pyunpack.Archive(os.path.join(dest, dl_file["name"])).extractall(dest)
    except:
        # The main error I saw was that zstd compressed data uses the flag
        # --long=31. We can't pass this flag to pyunpack so we need to run
        # zstd ourselves.
        logger.error("Pyunpack uncompression failed.", exc_info=True)
        if dl_file["name"].endswith(".zst"):
            logger.info("Extracting download to {dest} with tweaked zst.")
            compressed = os.path.join(dest, dl_file["name"])
            uncompressed = utils.zst_uncompress(compressed)
    return dest


def download_ia(wiki, dest_dir):
    """Download a wiki from the IA."""
    logger = logs.get_logger()
    if (ident := wiki["metadata"]["identifier"]) in utils.KNOWN_BAD:
        logger.warning(f"Skipping wiki as it is listed under utils.KNOWN_BAD")
        return None
    # There are multiple files that you can download, only downloaded the needed one.
    dl_file = utils.find_download(wiki)
    return download_and_extract(ident, dl_file, dest_dir)


def download_fandom(wiki, dest_dir):
    """TODO: Download wiki dumps directly from fandom."""
    logger = logs.get_logger()
    logger.warning(f"Fandom downloads not implemented yet, downloading from IA.")
    return download_ia(wiki, dest_dir)


def scrape_wiki(wiki, dest_dir):
    """TODO: Rescrape a wiki using wikiteam3."""
    logger = logs.get_logger()
    logger.warning(f"Wiki Re-scrapes not implemented yet, downloading from IA.")
    return download_ia(wiki, dest_dir)


def process_wiki(i, wiki, offset, dest_dir):
    """Download a wiki, proxying to different fetch functions based on the wiki."""
    logger = logs.get_logger()
    if "metadata" not in wiki:
        logger.error(
            f"Metadata missing from wiki, malformed record", extras={"line": i}
        )
        return None
    ident = wiki["metadata"]["identifier"]
    with logger(wiki=ident):
        if not utils.filter_language(lang := wiki["metadata"].get("language")):
            logger.warning(f"wiki appears to not be in english, found: {lang}")
            return None
        if not utils.check_alive(wiki):
            logger.info(f"wiki is offline, getting dump from IA.")
            return download_ia(wiki, dest_dir)
        if not utils.verify_license(wiki):
            logger.error(f"The IA license for wiki doesn't match the source.")
            return None
        if utils.check_fandom(wiki):
            logger.info(f"wiki is from fandom, downloading dump from them.")
            return download_fandom(wiki, dest_dir)
        if utils.check_wikimedia(wiki):
            logger.info(f"wiki is a WikiMedia wiki, us the `../dump` tools instead.")
            return None
        if utils.check_out_of_date(wiki, offset):
            logger.warning(f"IA dump of wiki is very out of date, re-scraping.")
            return scrape_wiki(wiki, dest_dir)


def main(args):
    logger = logs.get_logger()
    logger.info(f"Reading wiki metadata from {args.wiki_metadata}")
    with open(args.wiki_metadata) as f:
        wiki_metadata = [json.loads(l) for l in f if l]
    logger.info(f"{len(wiki_metadata)} wikis to download.")

    if args.test_run:
        logger.info(f"Test Run: Only downloading {args.test_run} wikis")
        # Not true shuffling as the number of permutations for so many wikis
        # is much larger than the period of the RNG (python breaks after ~2100)
        # but we aren't doing crypto so it isn't an issue.
        random.shuffle(wiki_metadata)
        wiki_metadata = wiki_metadata[: args.test_run]

    if args.num_workers and args.worker_id:
        # Partition downloads of different wikis to different workers.
        # Each worker runs their own copy of this script, with a unique
        # --worker_id
        wiki_metadata = [
            w
            for i, w in enumerate(wiki_metadata)
            if i % args.num_workers == args.worker_id
        ]
        logger.info(
            f"{len(wiki_metadata)} wikis to download as worker {args.worker_id}/{args.num_workers}."
        )

    # Run multiple download processes to try to have concurrent downloads from the IA
    # Will still be slow, especially if all workers are writing to a shared disk.
    with mp.Pool(args.num_threads) as pool:
        pool.starmap(
            functools.partial(process_wiki, offset=None, dest_dir=args.dest_dir),
            enumerate(wiki_metadata),
        )


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging()
    main(args)
