"""Convert the raw ubuntu data to the dolma format."""

import argparse
import datetime
import glob
import os
import urllib.parse

from charset_normalizer import from_bytes

from common_pile.licenses import PermissiveLicenses
from common_pile.logs import configure_logging, get_logger
from common_pile.write import to_dolma

SOURCE_NAME = "ubuntu-chat"
BASE_URL = "https://irclogs.ubuntu.com"

parser = argparse.ArgumentParser(description="Convert data to dolma.")
parser.add_argument(
    "--data",
    default="data/irclogs.ubuntu.com/",
    help="Path to the directory containing ubuntu chat data.",
)
parser.add_argument(
    "--output_dir",
    default=f"data/{SOURCE_NAME}/raw/documents/",
    help="Where the dolma formatted data goes.",
)
parser.add_argument(
    "--filename", default="ubuntu.jsonl.gz", help="The base filename for our chat data."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)


def format_dolma(chat: str, source_name: str = SOURCE_NAME, base_url: str = BASE_URL):
    # Manually split because os.path.split only give (head, tail)
    *_, year, month, day, channel = os.path.splitext(chat)[0].split(os.path.sep)
    created = datetime.date(int(year), int(month), int(day))
    logger = get_logger()
    logger.debug("Reading chat log from %s", chat)
    with open(chat, "rb") as f:
        # There is some encoding weirdness that this seems to fix.
        text = str(from_bytes(f.read()).best())
    return {
        # We don't want each channel to be it own data source so add the date
        # to the channel to make a unique string id.
        "id": f"{created.isoformat()}-{channel}",
        "text": text,
        "source": source_name,
        "added": datetime.datetime.utcnow().isoformat(),
        "created": created.isoformat(),
        "metadata": {
            "license": str(PermissiveLicenses.PD),
            # This will be added in the next phase of preprocessing.
            "authors": [],
            "url": urllib.parse.quote(
                f"{base_url}/{year}/{month}/{day}/{channel}.txt", safe=":/"
            ),
            "channel": channel,
        },
    }


def main(args):
    l = configure_logging()
    l.info(
        "Converting Chats from %s to the dolma format (at %s%s)",
        args.data,
        args.output_dir,
        args.filename,
    )
    # Use iterators so we don't have to load the whole dataset in memory.
    #                                                    year  month day   channel
    chats = map(
        format_dolma, glob.iglob(os.path.join(args.data, "**", "**", "**", "*.txt"))
    )
    to_dolma(chats, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
