import argparse
import functools
import multiprocessing as mp
import os
import tarfile

from tqdm import tqdm

cur_dir = os.getcwd()

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

BASE_URL = "https://ftp.ncbi.nlm.nih.gov/pub/pmc/"


def download(f_url: str, output_dir: str):
    # download file from f_url to output_dir
    try:
        # -nc: no clobber, -q: quiet
        os.system(f"wget -nc -q {f_url} -P {output_dir}")
    except:
        print(f"Error downloading {f_url}")
        import traceback

        traceback.print_exc()


def extract_and_convert_tarball(t: str, output_dir: str):
    if not os.path.exists(t):
        return
    try:
        with tarfile.open(t) as tar:
            files = tar.getnames()
            nxml = [f for f in files if f.endswith(".nxml")]

            # make sure there's only one nxml file
            assert len(nxml) == 1
            nxml = nxml[0]

            # extract nxml file
            tar.extract(nxml)

        # convert nxml to markdown
        pmcid = nxml.split("/")[0]
        os.system(f"pandoc --quiet -f jats {nxml} -o {pmcid}.md --wrap=none")

        # remove extracted files
        os.system(f"mv {pmcid}.md {output_dir} && rm -r {nxml.split('/')[0]}")

    except:
        print(f"Error extracting {t}")
        import traceback

        traceback.print_exc()
        import time

        time.sleep(0.1)


def download_and_convert(line: str, output_dir: str):
    # split line into parts
    partial_path, journal, PMCID, PMID, license = line.split("\t")

    # create paths for the url and the destination of the markdown file
    f_url = os.path.join(BASE_URL, partial_path)
    f_dest = os.path.join(output_dir, partial_path.split("/")[-1])

    try:
        download(f_url, output_dir)
        extract_and_convert_tarball(f_dest, output_dir)

        # delete the tarball
        os.system(f"rm {f_dest}")

    except Exception as e:
        print(e)


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
    main(args)
