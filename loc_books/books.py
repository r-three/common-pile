import os
import pandas as pd
import click
import requests
import sys

from time import time
from pyrate_limiter import Duration, Limiter, RequestRate
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from requests_ratelimiter import LimiterSession
from tqdm import tqdm
from pathlib import Path
from furl import furl

from licensed_pile import logs

file_directory = os.path.dirname(os.path.abspath(__file__))
download_folder = os.path.join(file_directory, "data/downloads/books")

logger = logs.get_logger("loc_books")

class LocBooksDownloader:
    def __init__(self, metadata_file):
        self.metadata_file = metadata_file
        self.burst_rate = RequestRate(40, Duration.SECOND * 10)
        self.crawl_rate = RequestRate(200, Duration.MINUTE)
        self.limiter = Limiter(self.burst_rate, self.crawl_rate)
        self.start = time()
        self.session = LimiterSession(limiter=self.limiter)
        self.progress_bar = tqdm(
            total=0, position=0, delay=1, desc="Downloading books"
        )

    def start_download(self):
        metadata = pd.read_csv(self.metadata_file)
        metadata_filtered = metadata.dropna()
        metadata_filtered = metadata_filtered[metadata_filtered["language"] == "english"]
        metadata_filtered = metadata_filtered[metadata_filtered["year"] > 1500]

        text_file_urls = metadata_filtered["text_file_url"]
        download_urls = self.urls_to_download(text_file_urls)
        self.progress_bar.total = len(download_urls)

        with PoolExecutor(max_workers=10) as executor:
            executor.map(self.download_book, download_urls)

    def urls_to_download(self, text_file_urls):
        existing_files = os.listdir(download_folder)
        files_to_download = []
        for text_file_url in text_file_urls:
            text_file_furl = furl(text_file_url)
            text_file_name = Path(str(text_file_furl.path)).name
            if text_file_name not in existing_files:
                files_to_download.append(text_file_url)

        return files_to_download

    
    def download_book(self, text_file_url):
        text_file_furl = furl(text_file_url)
        text_file_name = Path(str(text_file_furl.path)).name
        text_file_path = os.path.join(download_folder, text_file_name)
        self.progress_bar.set_description(f"{text_file_name}")
        if not os.path.exists(text_file_path):
            try:
                response = self.session.get(text_file_url)
                response.raise_for_status()
                text = response.text
                with open(text_file_path, "w") as f:
                    f.write(text)
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download {text_file_url}: {str(e)}")
            
        self.progress_bar.update(1)

@click.group()
def main():
    pass

@main.command()
@click.option("--metadata-file", required=True, help="Path to the metadata file")
def download(metadata_file):
    metadata_path = Path(metadata_file).absolute()
    downloader = LocBooksDownloader(metadata_path)
    downloader.start_download()


if __name__ == "__main__":
    logs.configure_logging("loc_books")
    main()
