"""Download wiki dumps from the internet archive."""

import argparse
import functools
import json
import multiprocessing.dummy as mp
import os
import subprocess
import re
import random

import internetarchive
import pyunpack
import utils

from licensed_pile import logs

parser = argparse.ArgumentParser(
    description="Download wiki dumps from the internet archive."
)
parser.add_argument("--wiki_metadata", default="data/ia-wikis.jsonl")
parser.add_argument("--test_run", type=int, help="")
parser.add_argument("--num_threads", type=int, default=32, help="")
parser.add_argument("--worker_id", type=int, help="")
parser.add_argument("--num_workers", type=int, help="")


# TODO: Default downloading to .../dumps
def download_and_extract(
    ident: str,
    dl_file,
    output_dir: str = "/fruitbasket/users/bdlester/projects/licensed_pile/wiki/archive/data/dumps",
    verbose: bool = True,
):
    logger = logs.get_logger("wiki/archive")
    dest = os.path.join(output_dir, ident)
    if os.path.exists(dest):
        logger.info(
            f"Skipping download of {dl_file['name']} for {ident} as {dest} already exists on disk."
        )
        return dest
    else:
        logger.info(f"Downloading {dl_file['name']} for {ident}.")
        try:
            internetarchive.download(
                ident, checksum=True, verbose=verbose, files=dl_file["name"], destdir=output_dir
            )
        except:
            logger.error(f"Failed to download {dl_file['name']}")
            return dest
    logger.info(f"Extracting download for {ident} to {dest}.")
    try:
        pyunpack.Archive(os.path.join(dest, dl_file["name"])).extractall(dest)
    except:
        if dl_file["name"].endswith(".zst"):
            with open(os.path.join(dest, re.sub(r"\.zst$", "", dl_file["name"])), "w") as wf:
               subprocess.run(["/usr/bin/zstd", "-c", "-d", "--long=31", "--", os.path.join(dest, dl_file["name"])], stdout=wf)
    return dest


def download_ia(wiki):
    logger = logs.get_logger("wiki/archive")
    if (ident := wiki["metadata"]["identifier"]) in utils.KNOWN_BAD:
        logger.warning(f"Skipping {ident} as it is listed under utils.KNOWN_BAD")
        return None
    dl_file = utils.find_download(wiki)
    return download_and_extract(ident, dl_file)


def download_fandom(wiki):
    logger = logs.get_logger("wiki/archive")
    logger.warning(f"Fandom downloads not implemented yet, downloading from IA.")
    return download_ia(wiki)


def download_wikimedia(wiki):
    logger = logs.get_logger("wiki/archive")
    logger.warning(f"Wikimedia downloads not implemented yet, downloading from IA.")
    return download_ia(wiki)


def scrape_wiki(wiki):
    logger = logs.get_logger("wiki/archive")
    logger.warning(f"Wiki Re-scrapes not implemented yet, downloading from IA.")
    return download_ia(wiki)


def process_wiki(i, wiki, offset):
    logger = logs.get_logger("wiki/archive")
    if "metadata" not in wiki:
        logger.error(f"Metadata missing from line {i}, malformed record")
        return None
    ident = wiki["metadata"]["identifier"]
    if not utils.filter_language(wiki["metadata"].get("language")):
        lang = wiki["metadata"].get("language")
        logger.warning(f"{ident} appears to not be english, found: {lang}")
        return None
    if not utils.check_alive(wiki):
        logger.info(f"{ident} is offline, getting dump from IA.")
        return download_ia(wiki)
    if not utils.verify_license(wiki):
        logger.error(f"The IA license for {ident} doesn't match the source.")
        return None
    if utils.check_fandom(wiki):
        logger.info(f"{ident} is a fandom wiki, downloading dump from there.")
        return download_fandom(wiki)
    if utils.check_wikimedia(wiki):
        logger.info(f"{ident} is a WikiMedia wiki, downloading dump from there.")
        return download_wikimedia(wiki)
    if utils.check_out_of_date(wiki, offset):
        logger.warning(f"IA dump for {ident} is very out of date, re-scraping.")
        return scrape_wiki(wiki)


# TODO: configure dest_dir
def main(args):
    logger = logs.get_logger("wiki/archive")
    logger.info(f"Reading wiki metadata from {args.wiki_metadata}")
    with open(args.wiki_metadata) as f:
        wiki_metadata = [json.loads(l) for l in f if l]
    logger.info(f"{len(wiki_metadata)} wikis to download.")

    if args.test_run:
        logger.info(f"Test Run: Only downloading {args.test_run} wikis")
        random.shuffle(wiki_metadata)
        wiki_metadata = wiki_metadata[: args.test_run]

    if args.num_workers and args.worker_id:
        wiki_metadata = [
            w for i, w in enumerate(wiki_metadata)
            if i % args.num_workers == args.worker_id
        ]
        logger.info(
            f"{len(wiki_metadata)} wikis to download as {args.worker_id}/{args.num_workers}."
        )

    with mp.Pool(args.num_threads) as pool:
        pool.starmap(
            functools.partial(process_wiki, offset=None), enumerate(wiki_metadata)
        )


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("wiki/archive")
    main(args)
