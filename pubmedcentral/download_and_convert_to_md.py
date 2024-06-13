import argparse
import functools
import json
import multiprocessing as mp
import os
import re
import shutil
import subprocess
import tarfile
import traceback
import xml.etree.ElementTree as ET

from tqdm import tqdm

from licensed_pile import logs
from licensed_pile.scrape import get_page

parser = argparse.ArgumentParser(description="Convert xml documents to markdown.")
parser.add_argument("--filelist", help="The path to the filelist.txt file.")
parser.add_argument(
    "--output_dir", default="data/md/", help="Where the markdown files go."
)
parser.add_argument(
    "--total_docs",
    default=0,
    type=int,
    help="Total number of documents to convert, for debugging.",
)
parser.add_argument(
    "--metadata_dir", default="data/metadata/", help="Where the metadata files go."
)
parser.add_argument(
    "--processes",
    default=mp.cpu_count(),
    type=int,
    help="Number of processes to use for conversion.",
)


def get_date_from_tree(tree):
    date_created = None
    # get date from tree
    # date can be found under a number of tags
    pub_types = ["pub", "epub", "pmc-release", "ppub"]
    for pub_type in pub_types:
        # try most common location first
        date = tree.find(f".//pub-date[@pub-type='{pub_type}']")
        if date is not None:
            # get year, month, and day
            # Use 1900-01-01 as default date
            # Try to get each component separately
            try:
                year = date.find("year").text
            except AttributeError:
                # if year is missing, use full default
                date_created = "1900-01-01"
                continue

            # if we found the year, try the month
            try:
                month = date.find("month").text
            except AttributeError:
                # if month is missing, use default month and date
                date_created = f"{year}-01-01"
                continue

            # if we found the month, try the day
            try:
                day = date.find("day").text
            except AttributeError:
                # if day is missing, use default day
                date_created = f"{year}-{month}-01"
                continue

            # If we successfully found all date components,
            #   convert to YYYY-MM-DD format
            date_created = f"{year}-{month}-{day}"
            break

        # try the next location
        date = tree.find(f".//pub-date[@date-type='{pub_type}']")
        if date is not None:
            # get year, month, and day
            # Use 1900-01-01 as default date
            # Try to get each component separately
            try:
                year = date.find("year").text
            except AttributeError:
                # if year is missing, use full default
                date_created = "1900-01-01"
                continue

            # if we found the year, try the month
            try:
                month = date.find("month").text
            except AttributeError:
                # if month is missing, use default month and date
                date_created = f"{year}-01-01"
                continue

            # if we found the month, try the day
            try:
                day = date.find("day").text
            except AttributeError:
                # if day is missing, use default day
                date_created = f"{year}-{month}-01"
                continue

            # If we successfully found all date components,
            #   convert to YYYY-MM-DD format
            date_created = f"{year}-{month}-{day}"
            break

    return date_created


def get_authors_and_date(nxml_file: str, pmcid: str):
    # get authors from nxml file
    authors = []
    date_created = None

    tree = ET.parse(nxml_file)

    # search for author tags
    for author in tree.findall(".//contrib[@contrib-type='author']"):
        surname = author.find("name/surname")
        given_names = author.find("name/given-names")
        if surname is not None and given_names is not None:
            authors.append({"first": given_names.text, "last": surname.text})

    # get date
    date_created = get_date_from_tree(tree)

    # occasionally, articles don't have a date within the tree
    # not a fatal error, just log it
    if date_created is None:
        logger = logs.get_logger("pubmedcentral")
        logger.info(
            f"Date not found for {pmcid}. Setting to default value of '1900-01-01'"
        )
        date_created = "1900-01-01"

    return authors, date_created


def download(f_url: str, output_dir: str):
    # download file from f_url to output_dir
    try:
        # get the tarball
        r = get_page(f_url)

        # write tarball to disk
        with open(os.path.join(output_dir, f_url.split("/")[-1]), "wb") as fh:
            fh.write(r.content)
    except:
        logger = logs.get_logger("pubmedcentral")
        logger.error(f"Error downloading {f_url}")
        logger.error(traceback.print_exc())


def extract_and_convert_tarball(t: str, output_dir: str):
    if not os.path.exists(t):
        return
    try:
        with tarfile.open(t) as tar:
            nxml = [f for f in tar.getnames() if f.endswith(".nxml")]

            # make sure there's only one nxml file
            if len(nxml) > 1:
                # haven't seen an example with more than one nxml file, but just in case
                error_message = f"More than one nxml file in {t}"
                logger = logs.get_logger("pubmedcentral")
                logger.error(error_message)
                raise ValueError(error_message)
            nxml = nxml[0]

            # extract nxml file
            tar.extract(nxml)

        # get pmcid
        pmcid = nxml.split("/")[0]

        # get metadata from nxml file
        authors, date_created = get_authors_and_date(nxml, pmcid)
        metadata = {"authors": authors, "created": date_created}
        # write to file
        with open(
            f"{os.path.join(args.metadata_dir, pmcid)}.json", "w", encoding="utf-8"
        ) as f:
            json.dump(metadata, f, ensure_ascii=False)

        # convert nxml to markdown
        # pandoc options:
        #   --quiet is to suppress messages
        #   --from jats specifies the input format as Journal Article Tag Suite (https://jats.nlm.nih.gov/)
        #   -o is the output file
        #   --wrap=none is to prevent pandoc from wrapping lines
        options = [
            "pandoc",
            "--quiet",
            "--from",
            "jats",
            nxml,
            "-o",
            f"{pmcid}.md",
            "--wrap=none",
        ]
        subprocess.run(options)

        # remove extracted files
        os.rename(f"{pmcid}.md", f"{output_dir}/{pmcid}.md")
        shutil.rmtree(nxml.split("/")[0], ignore_errors=True)

    except:
        logger = logs.get_logger("pubmedcentral")
        logger.error(f"Error extracting {t}")
        logger.error(traceback.print_exc())


def download_and_convert(
    line: str, output_dir: str, base_url="https://ftp.ncbi.nlm.nih.gov/pub/pmc/"
):
    # split line into parts
    partial_path = line.split("\t")[0]

    # create paths for the url and the destination of the markdown file
    f_url = os.path.join(base_url, partial_path)
    f_dest = os.path.join(output_dir, partial_path.split("/")[-1])

    try:
        download(f_url, output_dir)
        extract_and_convert_tarball(f_dest, output_dir)

        # delete the tarball
        os.remove(f_dest)

    except Exception as e:
        logger = logs.get_logger("pubmedcentral")
        logger.error(e)


def main(args):
    os.makedirs(args.output_dir, exist_ok=True)
    os.makedirs(args.metadata_dir, exist_ok=True)

    with open(args.filelist) as fh:
        files = fh.read().split("\n")

    # ignore the header
    files = files[1:]

    if args.total_docs > 0:
        files = files[: args.total_docs]

    with mp.Pool(args.processes) as p:
        # use list to force the execution of the imap iterable within the context of the multiprocessing pool
        _ = list(
            tqdm(
                p.imap(
                    functools.partial(download_and_convert, output_dir=args.output_dir),
                    files,
                ),
                total=len(files),
            )
        )


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("pubmedcentral")
    main(args)
