import argparse
import functools
import multiprocessing as mp
import os

from tqdm import tqdm

cur_dir = os.getcwd()

parser = argparse.ArgumentParser(description="Convert xml documents to markdown.")
parser.add_argument("--filelist", help="The path to the filelist.txt file.")
parser.add_argument(
    "--output_dir", default="data/raw/", help="Where the markdown files go."
)


def convert_xml_to_md(f: str, output_dir: str):
    filename = f.split("\t")[0]
    filepath = os.path.join(cur_dir, "data", filename)
    if not os.path.exists(filepath):
        print(f"File {filepath} does not exist.")
        return

    try:
        pmcid = filename.split("/")[-1].split(".")[0]
        os.system(f"pandoc -f jats {filepath} -o {pmcid}.md --wrap=none")
        os.system(f"mv {pmcid}.md {output_dir} && rm {filepath}")
    except:
        import traceback

        traceback.print_exc()
        import time

        time.sleep(0.1)


def main(args):
    filelist = args.filelist
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    with open(filelist) as fh:
        lines = fh.read().split("\n")

    p = mp.Pool(64)
    list(
        tqdm(
            p.imap(functools.partial(convert_xml_to_md, output_dir=output_dir), lines),
            total=len(lines),
        )
    )


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
