import argparse
import functools
import multiprocessing as mp
import os
import shutil
import subprocess
import tarfile
import traceback

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
                error_message = f"More than one nxml file in {t}"
                logger = logs.get_logger("pubmedcentral")
                logger.error(error_message)
                raise ValueError(error_message)
            nxml = nxml[0]

            # extract nxml file
            tar.extract(nxml)

        # convert nxml to markdown
        pmcid = nxml.split("/")[0]
        # --quiet is to suppress messages
        # --from jats specifies the input format as Journal Article Tag Suite (https://jats.nlm.nih.gov/)
        # -o is the output file
        # --wrap=none is to prevent pandoc from wrapping lines
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
    filelist = args.filelist
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    with open(filelist) as fh:
        lines = fh.read().split("\n")

    # ignore the header
    lines = lines[1:]

    if args.total_docs > 0:
        lines = lines[: args.total_docs]

    p = mp.Pool(64)

    pbar = tqdm(total=len(lines))
    for _ in p.imap(
        functools.partial(download_and_convert, output_dir=output_dir), lines
    ):
        pbar.update(1)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("pubmedcentral")
    main(args)
