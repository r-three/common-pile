import argparse
import os
import shutil
import multiprocessing as mp
import xml.etree.ElementTree as ET
import hashlib
import glob
import re
from collections import defaultdict

import jsonlines
from tqdm.auto import tqdm

from licensed_pile import logs
import utils
import parsers
import extract
from arxiv_id import ArXivID
from download import s3_download


parser = argparse.ArgumentParser(description="Convert Arxiv source files to the dolma format.")
parser.add_argument("--s3-endpoint-url", default="https://70ebd1e6ca0516b74feb79a54be9721c.r2.cloudflarestorage.com", help="URL for the S3 storage containing ArXiv source files")
parser.add_argument("--bucket-name", default="arxiv", help="Name of S3 bucket")
parser.add_argument("--manifest-path", default="src/arXiv_src_manifest.xml", help="Path to manifest file in the S3 bucket")
parser.add_argument("--metadata-path", default="data/arxiv-abstracts/raw/arxiv-metadata-oai-snapshot.json", help="Path to ArXiv metadata file")
parser.add_argument("--aws-profile", default="Eleuther-User", help="Name of profile to use in the AWS credentials file")
parser.add_argument("--n-procs", default=48, type=int, help="Number of processes")
parser.add_argument("--clean", default=True, action="store_true", help="Clean up extracted paper directories after run")
parser.add_argument("--binding-paths", nargs="+", default=["/fruitbasket/users/nkandpa2/projects/ar5iv-bindings/bindings", "/fruitbasket/users/nkandpa2/projects/ar5iv-bindings/supported_originals"], help="Paths to LaTeXML bindings")
parser.add_argument("--shard", type=int, required=True, help="Shard to process")
parser.add_argument("--num-shards", type=int, required=True, help="Number of shards to split job into")
parser.add_argument("--output-dir", required=True, help="Path to output directory")


def get_shard_num(s, num_shards):
    h = hashlib.sha256(s.encode())
    h_bytes = h.digest()
    h_int = int.from_bytes(h_bytes, byteorder="big")
    return h_int % num_shards


def get_permissive_ids(metadata_path):
    logger = logs.get_logger("arxiv-papers")

    logger.info(f"Finding permissively licensed papers from {metadata_path}")
    ids = set()
    with jsonlines.open(metadata_path, "r") as reader:
        for record in tqdm(reader):
            if utils.is_permissive(record.get("license")):
                id = record.get("id")
                if id is not None:
                    ids.add(ArXivID.from_string(id))
    logger.info(f"Found {len(ids)} permissive papers")
    return ids


def get_existing_ids(txt_dir):
    logger = logs.get_logger("arxiv-papers")
    
    logger.info(f"Finding papers that have already been parsed in {txt_dir}")
    ids = set()
    for yymm in [d for d in os.listdir(txt_dir) if re.match(r"[0-9]{4}", d) is not None]:
        for f in os.listdir(os.path.join(txt_dir, yymm)):
            m = re.match(r"(?P<id_string>.*).txt", f)
            if m is not None:
                ids.add(ArXivID.from_string(m.group("id_string")))
    logger.info(f"Found {len(ids)} papers in {txt_dir}")
    return ids


def get_manifest_files(endpoint_url, bucket_name, manifest_path, shard, num_shards, scratch_dir, arxiv_ids=set(), aws_profile=None):
    logger = logs.get_logger("arxiv-papers")
    
    download_path = os.path.join(scratch_dir, os.path.basename(manifest_path))
    s3_download(endpoint_url, bucket_name, manifest_path, download_path, aws_profile=aws_profile)
    
    arxiv_ids_dict = defaultdict(list)
    for arxiv_id in arxiv_ids:
        arxiv_ids_dict[arxiv_id.yymm].append(arxiv_id)

    manifest_files = []
    root = ET.parse(download_path)
    for file_element in root.findall(".//file"):
        filename = file_element.find("filename").text
        if get_shard_num(filename, num_shards) != shard:
            continue

        yymm = file_element.find("yymm").text
        yymm_ids = arxiv_ids_dict.get(yymm)
        if yymm_ids is not None:
            first_id = ArXivID.from_string(file_element.find("first_item").text)
            last_id = ArXivID.from_string(file_element.find("last_item").text)
            in_range_ids = [yymm_id for yymm_id in yymm_ids if yymm_id.id >= first_id.id and yymm_id.id <= last_id.id]
            logger.info(f"{yymm} {first_id.id}-{last_id.id} contains {len(in_range_ids)} permissive papers")
            if len(in_range_ids) > 0:
                manifest_files.append(filename)
        """
        if yymm in yymms:
            filename = file_element.find("filename").text
            if get_shard_num(filename, num_shards) == shard:
                manifest_files.append(filename)
        """
    return manifest_files 


def download_papers(endpoint_url, bucket_name, file_path, output_dir, aws_profile=None, overwrite=False):
    logger = logs.get_logger("arxiv-papers")
    
    download_path = os.path.join(output_dir, os.path.basename(file_path))
    s3_download(endpoint_url, bucket_name, file_path, download_path, aws_profile=aws_profile, overwrite=overwrite)
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
    
    permissive_ids = get_permissive_ids(args.metadata_path)
    existing_ids = get_existing_ids(txt_dir)
    ids_to_parse = permissive_ids - existing_ids
    logger.info(f"Found {len(ids_to_parse)} papers to parse")

    file_paths = get_manifest_files(args.s3_endpoint_url, args.bucket_name, args.manifest_path, args.shard, args.num_shards, scratch_dir, aws_profile=args.aws_profile, arxiv_ids=ids_to_parse)
    
    logger.info(f"Processing {len(file_paths)} tarballs with {args.n_procs} processes")
    if args.n_procs == 1:
        # Do this single process to make debugging easier
        for f in file_paths:
            process_tarball(args.s3_endpoint_url, args.bucket_name, f, scratch_dir, txt_dir, filter_ids=ids_to_parse, aws_profile=args.aws_profile, binding_paths=args.binding_paths, clean=args.clean)
    else:
        with mp.Pool(args.n_procs) as pool:
            args = [(args.s3_endpoint_url, args.bucket_name, f, scratch_dir, txt_dir, ids_to_parse, args.aws_profile, args.binding_paths, args.clean) for f in file_paths]
            pool.starmap(process_tarball, args)


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("arxiv-papers")
    main(args)
