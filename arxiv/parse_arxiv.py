import argparse
import os
import tarfile
import subprocess
from glob import glob
import io
import re
import shutil
import gzip
import multiprocessing as mp
import xml.etree.ElementTree as ET
import multiprocessing as mp
import functools

import jsonlines
import boto3
from tqdm.auto import tqdm

from licensed_pile import logs
import utils
import parsers
import extract
from arxiv_id import ArXivID


parser = argparse.ArgumentParser(description="Convert Arxiv source files to the dolma format.")
parser.add_argument("--s3-endpoint-url", default="https://70ebd1e6ca0516b74feb79a54be9721c.r2.cloudflarestorage.com", help="URL for the S3 storage containing ArXiv source files")
parser.add_argument("--bucket-name", default="arxiv", help="Name of S3 bucket")
parser.add_argument("--manifest-path", default="src/arXiv_src_manifest.xml", help="Path to manifest file in the S3 bucket")
parser.add_argument("--metadata-path", default="data/arxiv-abstracts/raw/arxiv-metadata-oai-snapshot.json", help="Path to ArXiv metadata file")
parser.add_argument("--aws-profile", default="Eleuther-User", help="Name of profile to use in the AWS credentials file")
parser.add_argument("--n-procs", default=48, type=int, help="Number of processes")
parser.add_argument("--clean", default=False, action="store_true", help="Clean up extracted paper directories after run")
parser.add_argument("--binding-paths", nargs="+", default=["/fruitbasket/users/nkandpa2/projects/ar5iv-bindings/bindings", "/fruitbasket/users/nkandpa2/projects/ar5iv-bindings/supported_originals"], help="Paths to LaTeXML bindings")
parser.add_argument("--output-dir", required=True, help="Path to output directory")


def get_permissive_ids(metadata_path):
    logger = logs.get_logger("arxiv-papers")

    logger.info(f"Finding permissively licensed papers from {metadata_path}")
    ids_to_parse = set()
    with jsonlines.open(metadata_path, "r") as reader:
        for record in tqdm(reader):
            if utils.is_permissive(record.get("license")):
                id = record.get("id")
                if id is not None:
                    ids_to_parse.add(ArXivID.from_string(id))
    logger.info(f"Found {len(ids_to_parse)} permissive papers")
    return ids_to_parse


def get_manifest_files(endpoint_url, bucket_name, manifest_path, output_dir, arxiv_ids=set(), aws_profile=None):
    logger = logs.get_logger("arxiv-papers")

    session = boto3.Session(profile_name=aws_profile)
    s3 = session.resource("s3", endpoint_url=endpoint_url)
    bucket = s3.Bucket(bucket_name)
    
    download_path = os.path.join(output_dir, os.path.basename(manifest_path))
    if not os.path.exists(download_path):
        logger.info(f"Downloading bucket manifest from {os.path.join(endpoint_url, bucket_name, manifest_path)}")
        bucket.download_file(manifest_path, download_path)
    
    yymms = set([arxiv_id.yymm for arxiv_id in arxiv_ids])
    manifest_files = []
    root = ET.parse(download_path)
    for file_element in root.findall(".//file"):
        yymm = file_element.find("yymm").text
        if yymm in yymms:
            manifest_files.append(file_element.find("filename").text)
    return manifest_files 


def download_papers(endpoint_url, bucket_name, file_path, output_dir, aws_profile=None, overwrite=False):
    logger = logs.get_logger("arxiv-papers")
    
    download_path = os.path.join(output_dir, os.path.basename(file_path))
    if os.path.exists(download_path) and not overwrite:
        logger.info(f"{download_path} already exists")
        return download_path

    logger.info(f"Downloading latex source tarball from {os.path.join(endpoint_url, bucket_name, file_path)}")
    session = boto3.Session(profile_name=aws_profile)
    s3 = session.resource('s3', endpoint_url=endpoint_url)
    bucket = s3.Bucket(bucket_name)
    download_path = os.path.join(output_dir, os.path.basename(file_path))
    bucket.download_file(file_path, download_path)
    return download_path


def parse_paper(paper_dir, output_dir, binding_paths=[]):
    logger = logs.get_logger("arxiv-papers")

    arxiv_id = ArXivID.from_paper_dir(paper_dir)
    output_file = os.path.join(output_dir, arxiv_id.yymm, f"{arxiv_id}.txt")
    if os.path.exists(output_file):
        logger.info(f"Skipping {arxiv_id} - {output_file} already exists")
        return

    parser = parsers.LaTeXMLParser(binding_paths=binding_paths)
    text = parser.parse(paper_dir)
    if text is None:
        parser = parsers.PyLaTeXEncParser()
        text = parser.parse(paper_dir)
    
    if text is None:
        logger.info(f"Failed to parse {paper_dir}")
        return

    logger.info(f"{arxiv_id}: Writing plaintext to {output_file}")
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, "w") as f:
        f.write(text)


def process_tarball(endpoint_url, bucket_name, file_path, scratch_dir, output_dir, filter_ids=set(), aws_profile=None, binding_paths=[], clean=False):
    download_path = download_papers(endpoint_url, bucket_name, file_path, scratch_dir, aws_profile=aws_profile)
    paper_dirs = extract.extract_papers(download_path, scratch_dir, filter_ids=filter_ids)
    for paper_dir in paper_dirs:
        parse_paper(paper_dir, output_dir, binding_paths)

    if clean:
        for paper_dir in paper_dirs:
            shutil.rmtree(paper_dir)


def main(args):
    logger = logs.get_logger("arxiv-papers")

    logger.info(f"Creating output directories at {args.output_dir}")
    scratch_dir = os.path.join(args.output_dir, "scratch")
    txt_dir = os.path.join(args.output_dir, "txt")
    os.makedirs(scratch_dir, exist_ok=True)
    os.makedirs(txt_dir, exist_ok=True)

    ids_to_parse = get_permissive_ids(args.metadata_path)
    file_paths = get_manifest_files(args.s3_endpoint_url, args.bucket_name, args.manifest_path, scratch_dir, aws_profile=args.aws_profile, arxiv_ids=ids_to_parse)
    
    logger.info(f"Processing {len(file_paths)} tarballs with {args.n_procs} processes")
    if args.n_procs == 1:
        # Do this single process to make debugging easier
        for f in file_paths:
            process_tarball(args.s3_endpoint_url, args.bucket_name, f, scratch_dir, txt_dir, filter_ids=ids_to_parse, aws_profile=args.aws_profile, binding_paths=args.binding_paths, clean=args.clean)
    else:
        with mp.Pool(args.n_procs) as pool:
            args = [(args.s3_endpoint_url, args.bucket_name, f, scratch_dir, txt_dir, ids_to_parse, args.aws_profile, args.binding_paths, args.clean) for f in file_paths]
            pool.starmap(process_tarball, args)

    if args.clean:
        for paper_dir in paper_dirs:
            logger.info(f"Cleaning scratch directory {paper_dir}")
            shutil.rmtree(paper_dir)

if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("arxiv-papers")
    main(args)
