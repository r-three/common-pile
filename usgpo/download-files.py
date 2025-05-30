"""Downloads plaintext files from the USGPO GovInfo API"""
import argparse
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm.auto import tqdm
from utils import api_query

import jsonlines
import trafilatura
from bs4 import BeautifulSoup
from common_pile import logs
from common_pile.licenses import PermissiveLicenses
from common_pile.write import to_dolma


SOURCE_NAME = "usgpo"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True, help="GovInfo API key")
    parser.add_argument(
        "--links-file", required=True, help="Path to links file (jsonl)"
    )
    parser.add_argument(
        "--output-dir",
        default=f"data/{SOURCE_NAME}/v0",
        help="Path to output directory",
    )
    parser.add_argument(
        "--filename",
        default="usgpo.jsonl.gz",
        help="The base filename for the USGPO Dolma dataset",
    )
    parser.add_argument(
        "--shard-size", type=int, default=1, help="Size, in GB, for each shard"
    )
    parser.add_argument("--workers", type=int, default=10, help="Number of threads")
    args = parser.parse_args()
    return args


def download_file(api_key, file_url):
    response = api_query(file_url, headers=None, params={"api_key": api_key})
    text = response.text
    return text


def parse_html(html):
    # Most documents are pre-formatted text inside of the a <pre> tag
    # For the rest of the documents, we use trafilatura to extract to markdown
    soup = BeautifulSoup(html, "html.parser")
    pre_tag = soup.find("pre")
    if pre_tag:
        text = pre_tag.get_text()
    else:
        text = trafilatura.extract(html, output_format="markdown")
    return text


def construct_record(api_key, file):
    logger = logs.get_logger("usgpo")
    try:
        all_links = file.get("links")
        if all_links is None:
            return None

        file_url = all_links.get("txtLink")
        # Occassionally there will be multiple txtLinks pointing to the same URL. Just take the first.
        if isinstance(file_url, list):
            file_url = file_url[0]

        if file_url is None:
            return None

        html = download_file(api_key, file_url)
        text = parse_html(html)

        if text is None or len(text) == 0:
            return None

        return {
            "id": file["package_id"],
            "created": file["date"],
            "text": text,
            "source": SOURCE_NAME,
            "added": datetime.datetime.utcnow().isoformat(),
            "metadata": {
                "title": file["title"],
                "author": file["author"],
                "publisher": file["publisher"],
                "category": file["category"],
                "license": str(PermissiveLicenses.PD),
                "url": file_url,
            },
        }

    except Exception as e:
        logger.error(f"Failed to download package {file['package_id']}: {e}")
        return None


def generate_records(args):
    with jsonlines.open(args.links_file, mode="r") as reader:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [
                executor.submit(construct_record, args.api_key, file) for file in reader
            ]
            for future in as_completed(futures):
                record = future.result()
                if record is not None:
                    yield record


def main(args):
    to_dolma(generate_records(args), args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("usgpo")
    main(args)
