import argparse
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import jsonlines
from licensed_pile import logs
from tqdm.auto import tqdm
from utils import api_query


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-key", required=True, help="GovInfo API key")
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date in ISO8601 format (yyyy-MM-dd'T'HH:mm:ss'Z')",
    )
    parser.add_argument("--output-dir", required=True, help="Path to output directory")
    parser.add_argument("--workers", type=int, default=20, help="Number of threads")
    parser.add_argument(
        "--collections",
        nargs="+",
        default=tuple(
            [
                "BILLS",
                "BUDGET",
                "CDIR",
                "CFR",
                "CPD",
                "CRI",
                "CZIC",
                "GAOREPORTS",
                "GPO",
                "HJOURNAL",
                "HOB",
                "PAI",
                "PLAW",
                "USCODE",
            ]
        ),
    )
    args = parser.parse_args()
    return args


def get_packages(api_key, collections, start_date):
    logger = logs.get_logger("usgpo")

    url = f"https://api.govinfo.gov/published/{start_date}"
    # offset mark is initially "*" to denote no offset
    offset_mark = "*"
    packages = []
    pbar = tqdm()
    while url is not None:
        response = api_query(
            url,
            headers={"accept": "application/json"},
            params={
                "api_key": args.api_key,
                "offsetMark": offset_mark,
                "pageSize": 1000,
                "collection": ",".join(collections),
            },
        )
        if response.status_code == 200:
            output = response.json()

            for record in output["packages"]:
                packages.append(record)
                pbar.update(1)

            url = output["nextPage"]
            offset_mark = None
            # Sleep since a sudden burst of requests seems to result in erroneous rate-limiting
            time.sleep(5)
        else:
            logger.error(
                f"get_packages received status code {response.status_code} for query {url}"
            )
            break
    return packages


def get_file_links(api_key, package):
    package_id = package["packageId"]
    response = api_query(
        f"https://api.govinfo.gov/packages/{package_id}/summary",
        headers={"accept": "application/json"},
        params={"api_key": args.api_key},
    )
    if response.status_code == 200:
        output = response.json()
        return output.get("download")
    return None


def get_package_metadata(api_key, package):
    record = {
        "title": package.get("title"),
        "package_id": package.get("packageId"),
        "date": package.get("dateIssued"),
        "category": package.get("category"),
        "author": package.get("governmentAuthor1"),
        "publisher": package.get("publisher"),
        "links": get_file_links(api_key, package),
    }
    return record


def main(args):
    logger = logs.get_logger("usgpo")
    os.makedirs(args.output_dir, exist_ok=True)

    # Get packages from the specified USGPO collections from `args.start_date` to current day
    logger.info(f"Getting packages from the following collections: {args.collections}")
    packages = get_packages(args.api_key, args.collections, args.start_date)

    logger.info(f"Getting package metadata and writing out to {args.output_dir}")
    with jsonlines.open(
        os.path.join(args.output_dir, "links.jsonl"), mode="w", flush=True
    ) as writer:
        # Spawn multiple worker threads to get the metadata associated with all packages
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            metadata_futures_to_package = {
                executor.submit(get_package_metadata, args.api_key, package): package
                for package in packages
            }

            # Write out package metadata to file
            for metadata_future in tqdm(as_completed(metadata_futures_to_package)):
                package = metadata_futures_to_package[metadata_future]
                try:
                    record = metadata_future.result()
                except Exception as e:
                    logger.error(f"Package {package} raised exception {e}")
                    continue
                writer.write(record)


if __name__ == "__main__":
    args = parse_args()
    logs.configure_logging("usgpo")
    main(args)
