"""Convert documents from Regulations.gov to plaintext"""

import argparse
import html
import itertools
import json
import os
import shutil
import subprocess
from collections import defaultdict

import requests
from bs4 import BeautifulSoup
from tqdm.auto import tqdm

from common_pile import logs


def parse_args():
    parser = argparse.ArgumentParser(description="Regulations.gov plaintext conversion")
    parser.add_argument(
        "--input-dir", required=True, help="Path to directory containing files"
    )
    parser.add_argument("--output-dir", required=True, help="Path to output directory")
    parser.add_argument(
        "--years",
        nargs="+",
        default=list(map(str, range(2000, 2024))),
        help="Years of data to process",
    )
    parser.add_argument(
        "--agencies",
        nargs="+",
        default=[
            "bis",
            "dot",
            "epa",
            "faa",
            "fda",
            "fema",
            "ferc",
            "fmcsa",
            "fra",
            "nhtsa",
            "osha",
            "phmsa",
            "sec",
            "uscg",
        ],
        help="Agencies to process",
    )
    parser.add_argument(
        "--file-types",
        nargs="+",
        default=[".txt", ".htm", ".doc", ".docx"],
        help="File types to convert to .txt",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        default=False,
        help="Overwrite existing files in output directory",
    )
    return parser.parse_args()


def convert_html(input_path, output_path):
    try:
        with open(input_path, "r") as f:
            text = f.read()
    except UnicodeDecodeError:
        try:
            with open(input_path, "r", encoding="windows-1252") as f:
                text = f.read()
        except:
            logger.error(f"Failed to read {input_path}")
            return
    soup = BeautifulSoup(text, "html.parser")
    pre_tag = soup.find("pre")
    if pre_tag:
        parsed_text = pre_tag.get_text()
    else:
        parsed_text = text

    parsed_text = html.unescape(parsed_text)

    with open(output_path, "w") as f:
        f.write(parsed_text)


def convert_doc(input_path, output_path):
    with open(output_path, "w") as f:
        try:
            subprocess.run(["catdoc", input_path], stdout=f, timeout=60)
        except subprocess.TimeoutExpired:
            logger.error(f"catdoc timed out for {input_path}")
            return


def convert_docx(input_path, output_path):
    try:
        # Produces a plain text file with the same name as the input file
        subprocess.run(["docx2txt", input_path], timeout=60)
    except subprocess.TimeoutExpired:
        logger.error(f"docx2txt timed out for {input_path}")
        return
    os.rename(f"{os.path.splitext(input_path)[0]}.txt", output_path)


def convert_txt(input_path, output_path):
    shutil.copyfile(input_path, output_path)


def main(args):
    logger = logs.get_logger("regulations")
    convert_funcs = {
        ".txt": convert_txt,
        ".htm": convert_html,
        ".doc": convert_doc,
        ".docx": convert_docx,
    }

    pbar = tqdm()
    postfix = defaultdict(lambda: 0)
    for year, agency in tqdm(itertools.product(args.years, args.agencies)):
        logger.info(f"Converting {agency} files from {year}")
        input_dir = os.path.join(args.input_dir, year, agency)
        output_dir = os.path.join(args.output_dir, year, agency)
        os.makedirs(output_dir, exist_ok=True)

        for file in os.listdir(input_dir):
            file_name, file_type = os.path.splitext(file)
            input_file = os.path.join(input_dir, file)
            output_file = os.path.join(output_dir, f"{file_name}.txt")

            if os.path.exists(output_file) and not args.overwrite:
                postfix["Skipped"] += 1
                logger.info(f"Skipping {output_file} (already exists)")
            elif file_type in args.file_types:
                func = convert_funcs[file_type]
                func(input_file, output_file)
                postfix[file_type] += 1
            else:
                postfix["Skipped"] += 1
                logger.error(f"Unsupported file type {file_type} for {input_file}")

            pbar.set_postfix(postfix)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("regulations")
    main(args)
