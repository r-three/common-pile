"""Code to download arxiv sources."""

import argparse
import bisect
import dataclasses
import hashlib
import operator as op
import os
import re
import tarfile

import smart_open
import tqdm

import licensed_pile.xml as xml
from licensed_pile import logs

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
parser.add_argument("--output_dir", default="data/", help="Where to save the shards.")
parser.add_argument(
    "--test_run",
    action="store_true",
    help="Is this a test run where we should only download a few shards?",
)
parser.add_argument(
    "--dry_run", action="store_true", help="Don't actually download any files."
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously downloaded shards?",
)
parser.add_argument(
    "--download_old",
    action="store_true",
    help="Should we download all the shards that use the old id format.",
)
parser.add_argument(
    "--download_manifest", action="store_true", help="Should we download the manifest."
)
parser.add_argument(
    "--manifest_only", action="store_true", help="Should we ONLY download the manifest."
)


@dataclasses.dataclass
class Shard:
    file_name: str
    start: str
    end: str
    md5: str


def parse_shard(shard) -> Shard:
    file_name = None
    md5_target = None
    start = None
    end = None
    for child in shard.iter():
        if child.tag == "filename":
            file_name = child.text
        elif child.tag == "md5sum":
            md5_target = child.text
        elif child.tag == "first_item":
            start = child.text
        elif child.tag == "last_item":
            end = child.text
    # If any were not found (de Morgan's law)
    if not (file_name and md5_target and start and end):
        logger = logs.get_logger("arxiv")
        logger.error(
            f"Filed to find filename={filename}, md5 hash={md5_target}, start={start}, or end={end}."
        )
        return None
    return Shard(file_name=file_name, start=start, end=end, md5=md5_target)


def get_manifest(url: str = MANIFEST, filename: str = "data/arXiv_src_manifest.xml"):
    transport_params = {
        "client_kwargs": {"S3.Client.get_object": {"RequestPayer": "requester"}}
    }
    logger = logs.get_logger("arxiv")
    logger.info(f"Downloading manifest from {url}")
    with smart_open.open(url, mode="rb", transport_params=transport_params) as f:
        manifest = f.read()
    logger.info(f"Saving manifest to {filename}")
    with open(filename, "wb") as wf:
        wf.write(manifest)


def is_new_id(article_id: str) -> bool:
    """Is the article Id the new"""
    return bool(re.match(r"\d{4}\.\d{5}", article_id))


class BulkDownloader:
    def __init__(
        self,
        manifest,
        output_dir: str,
        overwrite: bool = True,
        dry_run: bool = False,
        base_url: str = BASE_URL,
    ):
        self.manifest = manifest
        self.shards = sorted(
            [
                s
                for x in xml.iterate_xml(self.manifest, "file")
                if (s := parse_shard(x))
            ],
            key=op.attrgetter("start"),
        )
        # Used to look up which shard an id falls into.
        self.shard_starts = [s.start for s in self.shards]
        self.overwrite = overwrite
        self.base_url = base_url
        self.output_dir = output_dir
        self.dry_run = dry_run

    # To make testing easier.
    def find_shard(self, article_id: str) -> Shard:
        logger = logs.get_logger("arxiv")
        if not is_new_id(article_id):
            logger.warning(
                f"{article_id} uses the old id format, shard finding not currently supported."
            )
            return None
        index = bisect.bisect_right(self.shard_starts, article_id)
        if index == 0:
            logger.error(f"{article_id} appears too small for any shard")
            return None
        shard = self.shards[index - 1]
        if article_id > shard.end:
            logger.error(f"{article_id} appears too big for shard: {shard}")
        logger.debug(f"{article_id} should live in shard: {shard}")
        return shard

    def download(self, article_id: str):
        if shard := self.find_shard(article_id):
            self.download_shard(shard)

    def download_shard(self, shard: Shard):
        url = f"{self.base_url}/{shard.file_name}"
        logger = logs.get_logger("arxiv")
        logger.info(f"Downloading {url}")
        # Don't overwrite if it is already downloaded (and --overwrite isn't set.)
        output_file = os.path.join(self.output_dir, shard.file_name)
        if os.path.exists(output_file) and not self.overwrite:
            logger.info(f"Downloading of {url} skipped as the file already exists.")
            return
        if self.dry_run:
            logger.info(f"Downloading of {url} skipped as --dry_run was set.")
            return
        transport_params = {
            "client_kwargs": {"S3.Client.get_object": {"RequestPayer": "requester"}}
        }
        with smart_open.open(url, mode="rb", transport_params=transport_params) as f:
            data = f.read()
        # Check that the download wasn't corrupted.
        md5 = hashlib.md5(data).hexdigest()
        if md5 != shard.md5:
            logger.warning(f"md5 hash did not match for {shard.file_name}")
            return
        # Save the download compressed/unextracted (makes it easier to avoid
        # duplicate downloads.)
        logger.info(f"Saving shard to {output_file}")
        with open(output_file, "wb") as wf:
            wf.write(data)
        # Extract the tarball. We extract it into the same dir that the tarball
        # is in, we use os.path.dirname instead of the output_dir parameter
        # as the shards have an extra `src/` directory in their names.
        logger.info(f"Extracting shard at {output_file}")
        with tarfile.open(output_file) as tar:
            # TODO: If we move to python 3.12, add `filter="data"`
            tar.extractall(os.path.dirname(output_file))

    def download_all(self):
        for shard in self.shards:
            self.download_shard(shard)


def main(args):
    os.makedirs(os.path.join(args.output_dir, "src"), exist_ok=True)
    logger = logs.get_logger("arxiv")
    if (
        not os.path.exists(args.manifest)
        or args.download_manifest
        or args.manifest_only
    ):
        logger.info(f"Fetching manifest from {args.manifest_url}")
        get_manifest(args.manifest_url, args.manifest)
        if args.manifest_only:
            return 0

    bulk_downloader = BulkDownloader(
        manifest=args.manifest,
        output_dir=args.output_dir,
        overwrite=args.overwrite,
        dry_run=args.dry_run,
    )

    # Only download articles that have the old id format.
    if args.download_old:
        old_shards = [
            s
            for s in bulk_downloader.shards
            if not (is_new_id(s.start) and is_new_id(s.end))
        ]
        if args.test_run:
            old_shards = old_shards[:3]
        for shard in old_shards:
            bulk_downloader.download_shard(shard)
        return 0

    if args.test_run:
        for shard in bulk_downloader.shards[:3]:
            bulk_downloader.download_shard(shard)
    else:
        bulk_downloader.download_all()


if __name__ == "__main__":
    args = parser.parse_args()
    logs.configure_logging("arxiv")
    main(args)
