import argparse

from datetime import datetime
from tqdm.contrib.concurrent import process_map

import utils
from licensed_pile.write import to_dolma

parser = argparse.ArgumentParser(description="Download News Sites")
parser.add_argument(
    "--url", default="https://www.propublica.org/", help="Base URL"
)
parser.add_argument(
    "--output_dir",
    default="data/propublica/raw/",
    help="Path to output directory where raw pages are downloaded.",
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

def main(args):

    current_datetime = datetime.now()
    date = f"{current_datetime.year}-{current_datetime.month}-{current_datetime.day}"

    page_url_index = utils.build_url_index(args.url, keyword="article")

    def get_record(page_url):
        idx, url = page_url
        page_text = "\n".join(utils.get_text_from_page(url))

        return {
            "id": idx,
            "text": page_text,
            "source": url,
            "added": date,
            "metadata": {
                "license": "Creative Commons License (CC BY-NC-ND 3.0)",
            }
        }

    page_data = process_map(get_record, page_url_index, max_workers=10)
    to_dolma(list(page_data), args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
