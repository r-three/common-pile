import argparse

from datetime import datetime

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

    page_url_index = build_url_index(args.url, keyword_list="article")

    def get_record(page_url):
        idx, page_text = "\n".join(get_text_from_page(page_url))

        return {
            "id": idx,
            "text": page_text,
            "source": page_url,
            "added": date,
            "metadata": {
                "license": "Creative Commons License (CC BY-NC-ND 3.0)",
            }
        }

    page_data = map(get_record, page_url_index)
    to_dolma(list(page_data), args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
