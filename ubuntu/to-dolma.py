"""Convert the raw ubuntu data to the dolma format."""

import argparse
import datetime
import glob

from licensed_pile.licenses import PermissiveLicenses

SOURCE_NAME = "ubuntu-chat"
BASE_URL = "https://irclogs.ubuntu.com"

parser = argparse.ArgumentParser(description="Convert data to dolma.")
parser.add_argument(
    "--data", default="data", help="Path to the directory containing ubuntu chat data."
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
    *_, year, month, day, channel = os.path.split(os.path.splitext(chat)[0])
    created = datetime.date(year, month, day)
    with open(chat) as f:
        text = f.read()
    return {
        "id": f"{created.isoformat}-{channel}",
        "text": text,
        "source": source_name,
        "added": datetime.datetime.utcnow.isoformat(),
        "created": created.isoformat(),
        "metadata": {
            "license": str(PermissiveLicenses.PD),
            # TODO:
            "authors": [],
            # TODO: probably need to add url parse and a quote
            "url": f"{base_url}/{year}/{month}/{day}/{channel}.txt",
            "channel": channel,
        },
    }


def main(args):
    # Use iterators so we don't have to load the whole dataset in memory.
    chats = map(format_dolma, glob.iglob(os.path.join(args.data, "**", "*.txt")))
    to_dolma(chats, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)
