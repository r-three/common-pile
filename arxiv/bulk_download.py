"""Code to download arxiv sources."""

import argparse
import hashlib
import logging
import os

import smart_open
import tqdm

import licensed_pile.xml as xml

BASE_URL = "s3://arxiv"
MANIFEST = f"{BASE_URL}/src/arXiv_src_manifest.xml"

parser = argparse.ArgumentParser(description="Tool to download the arxiv bulk data.")
parser.add_argument(
    "--manifest",
    default="data/arXiv_src_manifest.xml",
    help="Path to the manifest file.",
)
parser.add_argument(
    "--manifest_url", default=MANIFEST, help="Where to download the manifest."
)
parser.add_argument(
    "--output_dir", default="data/src", help="Where to save the shards."
)
parser.add_argument(
    "--test_run",
    action="store_true",
    help="Is this a test run where we should only download a few shards?",
)
parser.add_argument(
    "--dry_run", action="store_true", help="Don't actually download any files."
)

logging.basicConfig(
    level=logging.INFO,
    format="arxiv bulk-download: [%(asctime)s] %(levelname)s - %(message)s",
)


def get_manifest(url: str = MANIFEST, filename: str = "data/arXiv_src_manifest.xml"):
    transport_params = {
        "client_kwargs": {"S3.Client.get_object": {"RequestPayer": "requester"}}
    }
    logging.info(f"Downloading manifest from {url}")
    with smart_open.open(url, mode="rb", transport_params=transport_params) as f:
        manifest = f.read()
    logging.info(f"Saving manifest to {filename}")
    with open(filename, "wb") as wf:
        wf.write(manifest)


def get_shard(
    shard_xml, output_dir: str, base_url: str = BASE_URL, dry_run: bool = False
):
    file_name = None
    md5_target = None
    for child in shard_xml.iter():
        if child.tag == "filename":
            file_name = child.text
        elif child.tag == "md5sum":
            md5_target = child.text
    if not (file_name and md5_target):
        logging.warning(f"Filed to find filename={filename} and md5 hash={md5_target}.")
        return
    url = f"{base_url}/{file_name}"
    logging.info(f"Downloading {url}")
    if dry_run:
        logging.info(f"Downloading of {url} skipped as --dry_run was set.")
        return
    transport_params = {
        "client_kwargs": {"S3.Client.get_object": {"RequestPayer": "requester"}}
    }
    with smart_open.open(url, mode="rb", transport_params=transport_params) as f:
        shard = f.read()
    md5 = hashlib.md5(shard).hexdigest()
    if md5 != md5_target:
        logging.warning(f"md5 hash did not match for {file_name}")
        return
    with open(os.path.join(output_dir, file_name), "wb") as wf:
        wf.write(shard)


def main(args):
    if not os.path.exists(args.manifest):
        logging.info("Manifest not found, fetching.")
        get_manifest(args.manifest_url, args.manifest)
    files = xml.iterate_xml(args.manifest, "file")
    for i, file_xml in enumerate(tqdm.tqdm(files)):
        get_shard(file_xml, args.output_dir, dry_run=args.dry_run)
        if i > 10 and args.test_run:
            break


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
