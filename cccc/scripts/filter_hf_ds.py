import argparse
import gzip
import json
import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
from pathlib import Path
from urllib.parse import urlparse

from tqdm import tqdm

# Filters the data to only include the URLs in the urls.txt file
# Requires a local copy of https://huggingface.co/datasets/allenai/dolma-cccc
# keeps the same folder structure in the destination directory


def setup_logging(log_level: str) -> None:
    """
    Configures the logging settings.
    """
    numeric_level = getattr(logging, log_level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f"Invalid log level: {log_level}")
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s [%(levelname)s] %(processName)s %(message)s",
        handlers=[logging.StreamHandler()],
    )


def read_file_to_list(file_path):
    with open(file_path, "r") as file:
        return [line.strip() for line in file]


def process_file(
    file_path: str, source_dir: str, dest_dir: str, urls: set
) -> bool | None:
    """
    Processes a single json.gz file by filtering its contents and saving it to the destination directory.
    """
    file_path = Path(file_path)
    source_dir = Path(source_dir)
    dest_dir = Path(dest_dir)

    # Compute the relative path to maintain directory structure
    rel_path = file_path.relative_to(source_dir)
    dest_file_path = dest_dir / rel_path

    # Ensure the destination directory exists
    dest_file_path.parent.mkdir(parents=True, exist_ok=True)

    # Open the source and destination files
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f_in, gzip.open(
            dest_file_path, "wt", encoding="utf-8"
        ) as f_out:
            for line in f_in:
                try:
                    item = json.loads(line)
                    if filter_condition(item, urls):
                        json.dump(item, f_out)
                        f_out.write("\n")
                except json.JSONDecodeError as e:
                    logging.error(f"Error decoding JSON in file {file_path}: {e}")
    except Exception as e:
        logging.error(f"Error processing file {file_path}: {e}")
        return False
    else:
        logging.info(f"Successfully processed file: {file_path}")
        return True


def extract_suburls(url: str) -> list:
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    path_parts = parsed_url.path.strip("/").split("/")

    suburls = [
        domain,
    ]
    current_path = domain

    for part in path_parts[:-1]:
        current_path = f"{current_path}/{part}"
        suburls.append(current_path)

    return suburls


def filter_condition(item: dict, URLS: set) -> bool:
    """Filters the data to only include the URLs in the urls.txt file"""
    return extract_suburls(item["metadata"]["warc_url"])[0] in URLS


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
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        help="Logging level (e.g., DEBUG, INFO, WARNING, ERROR).",
    )
    return parser


def main():
    args = setup_argparser().parse_args()

    setup_logging(args.log_level)

    URLS = set(read_file_to_list(args.urls_file))
    source_dir = Path(args.source_dir)
    dest_dir = Path(args.dest_dir)

    # Collect all json.gz files from the source directory and subdirectories
    files_to_process = list(source_dir.rglob("*.json.gz*"))

    num_processes = args.num_processes

    logging.info(f"Starting processing with {num_processes} processes...")
    logging.info(f"Total files to process: {len(files_to_process)}")

    # Use ProcessPoolExecutor to process files concurrently
    with ProcessPoolExecutor(max_workers=num_processes) as executor:
        # Use partial to fix arguments source_dir and dest_dir
        func = partial(
            process_file, source_dir=source_dir, dest_dir=dest_dir, urls=URLS
        )
        futures = {
            executor.submit(func, file_path): file_path
            for file_path in files_to_process
        }

        # Use tqdm to display progress bar
        for future in tqdm(
            as_completed(futures), total=len(futures), desc="Processing files"
        ):
            file_path = futures[future]
            try:
                result = future.result()
                if not result:
                    logging.error(f"Processing failed for file: {file_path}")
            except Exception as e:
                logging.error(f"Error occurred in file {file_path}: {e}")

    logging.info("Processing complete.")


if __name__ == "__main__":
    main()
