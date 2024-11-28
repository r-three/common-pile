import argparse
import logging
import os
from pathlib import Path
from tempfile import TemporaryDirectory
from urllib.parse import urlparse

from licensed_pile.write import ShardParallelProcessor

logger = logging.getLogger(__name__)

# Filters the data to only include the URLs in the urls.txt file
# Requires a local copy of https://huggingface.co/datasets/allenai/dolma-cccc
# keeps the same folder structure in the destination directory


class ShardCCProcessor(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        if filter_condition(example, kwargs["urls"]):
            return example
        else:
            return None


def read_file_to_list(file_path: str) -> list[str]:
    res = []
    with open(file_path, "r") as file:
        urls = [line.strip() for line in file]
    for x in urls:
        if x.startswith("www."):
            res.append(x.removeprefix("www."))
        else:
            res.append("www." + x)
        res.append(x)

    return res


def extract_suburls(url: str) -> str:
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    return domain


def filter_condition(item: dict, urls: set) -> bool:
    """Filters the data to only include the URLs in the urls.txt file"""
    return extract_suburls(item["metadata"]["warc_url"]) in urls


def setup_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Process and filter json.gz files based on URLs."
    )
    parser.add_argument(
        "--urls-file",
        type=str,
        required=True,
        help="Path to the newline delimited textfile containing URLs to keep.",
    )
    parser.add_argument(
        "--source-dir",
        type=str,
        required=True,
        help="Source directory containing json.gz files.",
    )
    parser.add_argument(
        "--dest-dir",
        type=str,
        required=True,
        help="Destination directory to save processed files.",
    )
    parser.add_argument(
        "--num-processes",
        type=int,
        default=os.cpu_count() - 1,
        help="Number of processes to use. Default is (CPU count - 1).",
    )
    return parser


def main():
    args = setup_argparser().parse_args()

    URLS = set(read_file_to_list(args.urls_file))
    source_dir = Path(args.source_dir)
    dest_dir = Path(args.dest_dir)

    # Collect all json.gz files from the source directory and subdirectories
    files_to_process = list(source_dir.rglob("*.json.gz*"))

    num_processes = args.num_processes

    logger.info(f"Starting processing with {num_processes} processes...")
    logger.info(f"Total files to process: {len(files_to_process)}")
    with TemporaryDirectory() as tempdir:
        processor = ShardCCProcessor(
            source_prefix=str(source_dir / "*" / "*.json.gz"),
            destination_prefix=str(dest_dir),
            metadata_prefix=tempdir,
            num_processes=num_processes,
        )
        processor(debug=args.debug, urls=URLS)


if __name__ == "__main__":
    main()
