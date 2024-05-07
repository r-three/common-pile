import json
import os
from concurrent.futures import ThreadPoolExecutor as PoolExecutor
from pathlib import Path
from time import time

import click
import pandas as pd
import requests
from dateutil.parser import parse
from furl import furl
from pyrate_limiter import Duration, Limiter, RequestRate
from requests_ratelimiter import LimiterSession
from titlecase import titlecase
from tqdm import tqdm

from licensed_pile import logs

file_directory = os.path.dirname(os.path.abspath(__file__))
downloads_folder = os.path.join(file_directory, "data/downloads/metadata")
filename_digits = 4

logger = logs.get_logger("loc_books")

class LocBooksMetadataDownloader:
    def __init__(self, base_url, download_folder):
        self.base_url = base_url
        self.download_folder = download_folder
        self.burst_rate = RequestRate(
            20, Duration.SECOND * 10
        )  # 10 requests per 10 seconds
        self.crawl_rate = RequestRate(80, Duration.MINUTE)  # 80 requests per minute
        self.limiter = Limiter(self.burst_rate, self.crawl_rate)
        self.start = time()
        self.date_facets = []
        self.items_per_page = 100
        self.session = LimiterSession(limiter=self.limiter)
        self.progress_bar = tqdm(
            total=0, position=0, delay=1, desc="Downloading metadata"
        )
        self.existing_pages_count = 0

    def start_download(self):
        base_furl = furl(self.base_url)
        base_furl.args["fo"] = "json"
        logger.info(f"Downloading metadata from URL: {base_furl.url}")
        try:
            response = self.session.get(base_furl.url)
            response.raise_for_status()
            json_data = response.json()
            self.parse_date_facets(json_data)
            for date_facet in self.date_facets:
                self.download_date_facet(date_facet)
        except requests.exceptions.RequestException as e:
            logger.error("Failed to download base URL:", str(e))

    def download_date_facet(self, date_facet):
        facet_folder = os.path.join(self.download_folder, date_facet["year_range"])
        facet_url = date_facet["link"]

        os.makedirs(facet_folder, exist_ok=True)

        self.download_page(facet_url, facet_folder, 1)
        with open(
            os.path.join(facet_folder, f"{str(1).zfill(filename_digits)}.json")
        ) as f:
            file_data = json.load(f)
            total_pages = file_data["pagination"]["total"]

        pages_to_download = self.check_existing_files(total_pages, facet_folder)
        self.existing_pages_count += total_pages - len(pages_to_download)
        self.progress_bar.total += len(pages_to_download)

        with PoolExecutor(max_workers=10) as executor:
            executor.map(
                self.download_page,
                [facet_url] * len(pages_to_download),
                [facet_folder] * len(pages_to_download),
                pages_to_download,
            )

    def parse_date_facets(self, json_data):
        date_facet_data = next(
            (facet for facet in json_data["facets"] if facet["type"] == "dates"), None
        )
        date_facets = []
        if date_facet_data:
            for filter in date_facet_data["filters"]:
                date = parse(filter["term"])
                start_year = date.year
                link = filter["on"]
                link_furl = furl(link)
                year_range = "-".join(link_furl.args["dates"].split("/"))
                date_facet = {
                    "start_year": start_year,
                    "year_range": year_range,
                    "count": filter["count"],
                    "link": filter["on"],
                }
                date_facets.append(date_facet)

        self.date_facets = date_facets

    def check_existing_files(self, total_pages, output_folder):
        pages_to_download = []
        for page in range(1, total_pages + 1):
            filename = os.path.join(
                output_folder, f"{str(page).zfill(filename_digits)}.json"
            )
            if not os.path.exists(filename):
                pages_to_download.append(page)
        return pages_to_download

    def download_page(self, facet_url, output_folder, page):
        facet_furl = furl(facet_url)
        facet_furl.args["c"] = self.items_per_page
        facet_furl.args["sp"] = page
        facet_furl.args["fo"] = "json"
        filename = os.path.join(
            output_folder, f"{str(page).zfill(filename_digits)}.json"
        )

        if not os.path.exists(filename):
            try:
                response = self.session.get(facet_furl.url)
                response.raise_for_status()
                with open(filename, "w", encoding="utf-8") as file:
                    file.write(response.text)
            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to download page {page}: {str(e)}")
        self.progress_bar.update(1)
        self.progress_bar.desc = f"{filename}"


class LocBooksMetadataParser:
    def __init__(self, download_folder):
        self.download_folder = download_folder
        self.total_data = []

    def parse_year(self, x):
        if isinstance(x, list):
            try:
                parsed_date = parse(x[0])
                year_string = parsed_date.strftime("%Y")
                return int(year_string)
            except Exception as e:
                return None
        else:
            if x is None:
                return None

    def parse_text_file_url(self, x):
        if isinstance(x, list):
            if len(x) > 0:
                if "djvu_text_file" in x[0]:
                    return x[0]["djvu_text_file"]
        else:
            return None

    def parse_files(self):
        json_files = []

        for root, dirs, files in os.walk(self.download_folder):
            for file in files:
                if file.endswith(".json"):
                    json_files.append(os.path.join(root, file))

        progress_bar = tqdm(json_files, desc="Processing metadata")

        for filepath in progress_bar:
            filename = os.path.basename(filepath).split(".")[0]
            progress_bar.set_description(f"{filepath}")

            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            df_in = pd.json_normalize(data["results"])
            df_out = pd.DataFrame(
                columns=[
                    "lccn",
                    "year",
                    "title",
                    "author",
                    "page_count",
                    "language",
                    "text_file_url",
                ]
            )

            df_out["lccn"] = df_in["number_lccn"].apply(
                lambda x: x[0] if isinstance(x, list) else x
            )
            df_out["title"] = df_in["title"]
            df_out["language"] = df_in["language"].apply(
                lambda x: x[0] if isinstance(x, list) else x
            )
            df_out["author"] = df_in["contributor"].apply(
                lambda x: titlecase(x[0]) if isinstance(x, list) else x
            )
            df_out["page_count"] = (
                df_in["segments"]
                .apply(
                    lambda x: x[0]["count"] if isinstance(x, list) and len(x) > 0 else 1
                )
                .astype("Int64")
            )
            df_out["year"] = df_in["dates"].apply(self.parse_year).astype("Int64")
            df_out["text_file_url"] = df_in["resources"].apply(self.parse_text_file_url)

            self.total_data.append(df_out)

        df = pd.concat(self.total_data, ignore_index=True)
        df.drop_duplicates(subset=["lccn"], inplace=True)

        return df


@click.group()
def main():
    pass


@main.command()
@click.option(
    "--base-url",
    required=True,
    help="Base URL for the data to be downloaded (including LoC catalog facets, etc.).",
    default="https://www.loc.gov/collections/selected-digitized-books/?fa=language:english",
)
@click.option(
    "--download-folder", required=True, help="Download folder for the data to be saved."
)
@click.option(
    "--date-range", required=False, help="Date range for the data to be downloaded."
)
def download(base_url, download_folder, date_range):
    if date_range:
        base_url = f"{base_url}&dates={date_range}"
        first_date = date_range.split("/")[0]
        last_date = date_range.split("/")[1]
        download_folder = os.path.join(download_folder, f"{first_date}-{last_date}")

    downloader = LocBooksMetadataDownloader(base_url, download_folder)
    downloader.start_download()
    logger.info(
        f"Downloaded {downloader.progress_bar.total} pages. {downloader.existing_pages_count} files already exist."
    )


@click.option(
    "--download-folder",
    required=True,
    help="Download folder for the data to be processed.",
)
@main.command()
def process(download_folder):
    parser = LocBooksMetadataParser(download_folder)
    df = parser.parse_files()
    filename = Path(download_folder).name + ".csv"
    processed_folder = os.path.join(file_directory, "data/processed/metadata")
    processed_csv = os.path.join(processed_folder, filename)
    df.to_csv(processed_csv, index=False)
    logger.info(f"Processed metadata saved to {processed_csv}")


if __name__ == "__main__":
    logs.configure_logging("loc_books")
    main()
