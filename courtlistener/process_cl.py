import argparse
import csv
import logging
import os
import re
import sys

import pandas as pd

from common_pile.licenses import PermissiveLicenses
from common_pile.logs import configure_logging
from common_pile.write import to_dolma

SOURCE_NAME = "CourtListenerOpinion"

csv.field_size_limit(sys.maxsize)

logger = configure_logging("court-listener-opinion")


def process_court_listener(file_path):
    df = pd.read_csv(file_path)

    # add metadata column
    df["metadata"] = str(PermissiveLicenses.PD)

    # add source column
    df["source"] = SOURCE_NAME

    """
    Remove columns:
    date_modified
    author_str
    per_curiam
    joined_by_str
    type
    sha1
    page_count
    local_path
    extracted_by_ocr
    author_id
    cluster_id
    """
    df = df.drop(
        columns=[
            "date_modified",
            "author_str",
            "per_curiam",
            "joined_by_str",
            "type",
            "sha1",
            "page_count",
            "local_path",
            "extracted_by_ocr",
            "author_id",
            "cluster_id",
        ]
    )

    """
    Merge columns based on Court Listener documentation:
    html_with_citations
    html_columbia
    html_lawbox
    xml_harvard
    html_anon_2020
    html
    plain_text
    """
    df["text"] = (
        df["html_with_citations"]
        .combine_first(df["html_columbia"])
        .combine_first(df["html_lawbox"])
        .combine_first(df["xml_harvard"])
        .combine_first(df["html_anon_2020"])
        .combine_first(df["html"])
    )

    # keep only the text columns and drop null values
    df = df.drop(
        columns=[
            "html",
            "html_anon_2020",
            "html_lawbox",
            "html_columbia",
            "xml_harvard",
            "html_with_citations",
        ]
    ).dropna(subset=["text"])

    # extract text from html and xml following Harvard CAP
    # They used r"<.+?>", ""
    df["text"] = df["text"].apply(lambda x: re.sub(r"<.+?>", "", x))

    # combine merge plain text and extracted text
    df["text"] = df["text"].combine_first(df["plain_text"])

    # drop plain text column and text null values
    df = df.drop(columns=["plain_text"]).dropna(subset=["text"])

    # return a dictionary for each row - dolma format
    return df.to_dict(orient="records")


def main(args):
    example = process_court_listener(args.input_file)
    output_file_base_name = os.path.basename(args.input_file).replace(
        ".csv", ".jsonl.gz"
    )
    to_dolma(example, args.output_dir, output_file_base_name, args.shard_size)
    logger.info(f"Saved {args.input_file} as dolma shared files at {args.output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Convert csv data to dolma.")
    parser.add_argument(
        "--output_dir",
        default=f"data/courtlistener/v0",
        help="Where the dolma formatted data goes.",
    )
    parser.add_argument(
        "--shard_size",
        default=1000,
        help="The number of documents to store in each shard.",
    )
    parser.add_argument(
        "--input_file",
        default="./data/courtlistener/raw/opinions-2022-08-02.csv",
        help="The path to the csv file to convert.",
    )
    args = parser.parse_args()
    main(args)
