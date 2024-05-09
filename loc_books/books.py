import datetime
import functools
import multiprocessing.dummy as mp
import os
import sys
from pathlib import Path
from time import time

import click
import pandas as pd
import requests
from furl import furl
from pyrate_limiter import Duration, Limiter, RequestRate
from requests_ratelimiter import LimiterSession
from tenacity import (
    RetryError,
    Retrying,
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
from tqdm import tqdm

from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

file_directory = os.path.dirname(os.path.abspath(__file__))
download_folder = os.path.join(file_directory, "data/downloads/books")

logger = logs.get_logger("loc_books")


class LocBooksDownloader:
    def __init__(self, metadata_file):
        self.metadata_file = metadata_file

        concurrent_rate = RequestRate(10, Duration.SECOND)
        burst_rate = RequestRate(80, Duration.SECOND * 10)
        crawl_rate = RequestRate(400, Duration.MINUTE)
        self.limiter = Limiter(concurrent_rate, burst_rate, crawl_rate)
        self.session = LimiterSession(limiter=self.limiter)

        self.progress_bar = tqdm(total=0, position=0, delay=1, desc="Downloading books")

    def start_download(self):
        metadata = pd.read_csv(self.metadata_file)
        metadata_filtered = metadata.dropna()
        metadata_filtered = metadata_filtered[
            metadata_filtered["language"] == "english"
        ]
        metadata_filtered = metadata_filtered[metadata_filtered["year"] >= 1500]

        text_file_urls = metadata_filtered["text_file_url"]
        print(f"Found {len(text_file_urls)} books in metadata file")
        download_urls = self.urls_to_download(text_file_urls)
        print(f"{len(text_file_urls) - len(download_urls)} books already downloaded")
        print(f"Downloading {len(download_urls)} books")
        self.progress_bar.total = len(download_urls)

        with mp.Pool(20) as pool:
            results = pool.imap(functools.partial(self.download_book), download_urls)
            for result in results:
                if result:
                    self.progress_bar.set_description(f"{result['filename']}")
                    self.progress_bar.update(1)
                pass

    def urls_to_download(self, text_file_urls):
        existing_files = set(
            [path.name for path in Path(download_folder).glob("*.txt")]
        )
        download_urls = [
            text_file_url
            for text_file_url in text_file_urls
            if Path(str(furl(text_file_url).path)).name not in existing_files
        ]

        return download_urls

    def download_book(self, text_file_url):
        text_file_furl = furl(text_file_url)
        text_file_name = Path(str(text_file_furl.path)).name
        text_file_path = os.path.join(download_folder, text_file_name)
        if not os.path.exists(text_file_path):
            try:
                for attempt in Retrying(
                    stop=stop_after_attempt(3),
                    wait=wait_random_exponential(multiplier=1, max=30),
                ):
                    with attempt:
                        try:
                            response = self.session.get(text_file_url)
                            response.raise_for_status()
                            text = response.text
                            with open(text_file_path, "w") as f:
                                f.write(text)
                            return {
                                "success": True,
                                "url": text_file_url,
                                "filename": text_file_name,
                                "text": text,
                            }
                        except requests.exceptions.RequestException as e:
                            logger.error(
                                f"Failed to download {text_file_url}: {str(e)}"
                            )
                            raise e
            except RetryError:
                logger.error(f"Failed to download {text_file_url} after 3 attempts")
                return {
                    "success": False,
                    "url": text_file_url,
                    "filename": text_file_name,
                    "text": None,
                }

        return None


class LocBooksProcessor:
    def __init__(self, metadata_file):
        self.metadata = pd.read_csv(metadata_file)
        self.metadata["text_file_name"] = self.metadata["text_file_url"].apply(
            lambda x: Path(str(furl(x).path)).name
        )

    def start_processing(self, shard_size, filename):
        text_files = list(Path(download_folder).glob("*.txt"))
        print(f"Found {len(text_files)} text files")
        results = map(functools.partial(self.format_dolma), text_files)
        to_dolma(results, "data/processed/books", filename, shard_size)

    def format_dolma(self, filepath):
        filename = filepath.name
        metadata = self.metadata.loc[self.metadata["text_file_name"] == filename]
        if not metadata.empty:
            with open(filepath) as f:
                text = f.read()

            dolma_data = {
                "id": metadata["lccn"].values[0],
                "text": text,
                "source": "loc_books",
                "added": datetime.datetime.utcnow().isoformat(),
                "metadata": {
                    "license": str(PermissiveLicenses.PD),
                    "title": metadata["title"].values[0],
                    "author": metadata["author"].values[0],
                    "year": int(metadata["year"].values[0]),
                    "language": metadata["language"].values[0],
                    "url": metadata["text_file_url"].values[0],
                },
            }

            return dolma_data


@click.group()
def main():
    pass


@main.command()
@click.option("--metadata-file", required=True, help="Path to the metadata file")
def download(metadata_file):
    metadata_path = Path(metadata_file).absolute()
    downloader = LocBooksDownloader(metadata_path)
    downloader.start_download()


@main.command()
@click.option("--metadata-file", required=True, help="Path to the metadata file")
@click.option("--shard-size", default=1, help="File size in GB")
@click.option(
    "--filename",
    required=True,
    default="loc_books.jsonl.gz",
    help="The base filename for LOC books",
)
def process(metadata_file, shard_size, filename):
    metadata_path = Path(metadata_file).absolute()
    processor = LocBooksProcessor(metadata_path)
    processor.start_processing(shard_size, filename)


if __name__ == "__main__":
    logs.configure_logging("loc_books")
    main()
