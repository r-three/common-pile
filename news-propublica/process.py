import argparse
import multiprocessing as mp

from tqdm import tqdm
from functools import partial
from datetime import datetime

import utils
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Download News Sites")
parser.add_argument(
    "--url", default="https://www.propublica.org/", help="Base URL"
)
parser.add_argument(
    "--output_dir",
    default="data/news-propublica/",
    help="Path to output directory where raw pages are downloaded.",
)
parser.add_argument(
    "--version",
    type=int,
    default=1,
    help="Version of the subset",
)
parser.add_argument(
    "--index_file",
    default=None,
    help="File that list of all pages",
)
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously downloaded copies?",
)
parser.add_argument(
    "--filename", default="pro.jsonl.gz", help="The base filename for our books."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)
parser.add_argument(
    "--num_workers",
    default=None,
    help="Number of workers",
)

def get_record(page_url, date=None):
    idx, url = page_url
    page_text = utils.get_text_from_page(url)

    return {
        "id": idx,
        "text": page_text,
        "source": url,
        "added": date,
        "metadata": {
            "license": "Creative Commons License (CC BY-NC-ND 3.0)",
        }
    }

def main(args):

    current_datetime = datetime.now()
    date = f"{current_datetime.year}-{current_datetime.month}-{current_datetime.day}"

    if args.index_file:
        page_index = args.index_file
    else:
        page_index = utils.build_url_index(args.url, keyword="article")

    page_index = [(idx, page) for idx, page in enumerate(page_index)]
    
    num_workers = mp.cpu_count() if args.num_workers is None else args.num_workers
    with mp.Pool(num_workers) as p:
        page_data = list(p.map(partial(get_record, date=date), tqdm(page_index)))

    # Raw Version
    raw_output_dir = os.path.join(args.output_dir, "raw")
    # Save to SOURCE/Raw/

    # Do clean up process

    # Cleaned Version
    cleaned_output_dir = os.path.join(args.output_dir, f"v{version}")
    to_dolma(page_data, cleaned_output_dir, args.filename, args.shard_size)
    return 0


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
