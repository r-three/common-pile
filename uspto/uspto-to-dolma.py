import argparse
from functools import partial

import datasets
from download_preprocess import format_text

from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma


def get_dataset(
    hf_path: str, cache_dir: str | None = None, streaming: bool = False
) -> datasets.Dataset:
    kwargs = dict(split="train")
    if cache_dir:
        kwargs["cache_dir"] = cache_dir
    if streaming:
        kwargs["streaming"] = True
    uspto_df = datasets.load_dataset(hf_path, **kwargs)
    return uspto_df


def return_dolma(ds: datasets.Dataset) -> dict[str, str]:
    for x in ds:
        output = {
            "text": x.get("text"),
            "id": x.get("application_number"),
            "source": "Google Patents Public Data",
            "metadata": {
                "license": str(PermissiveLicenses.CC_BY),
                "language": x.get("title_language"),
                "publication_date": str(x.get("publication_date")),
            },
        }
        yield output


parser = argparse.ArgumentParser()
parser.add_argument(
    "--output_dir", type=str, help="Output directory", default=r"/data/uspto/raw"
)
parser.add_argument(
    "--dataset", type=str, help="Path to raw HF dataset", default=r"baber/USPTO"
)
parser.add_argument(
    "--cache_dir",
    type=str,
    help="Path to cache HF dataset",
    default=r"./data/uspto/raw",
)
parser.add_argument("--streaming", action="store_true")
parser.add_argument(
    "--url",
    type=str,
    help="REST API URL for the Node.js MathML to LaTeX converter",
    default=r"http://localhost:3000/convert",
)

if __name__ == "__main__":
    args = parser.parse_args()
    URL = args.url
    DATASET = args.dataset
    OUTPUT_DIR = args.output_dir
    uspto_df = get_dataset(DATASET, cache_dir=args.cache_dir, streaming=args.streaming)
    format_text = partial(format_text, URL)
    uspto_df = uspto_df.map(format_text, remove_columns=list(uspto_df.column_names))
    to_dolma(return_dolma(uspto_df), OUTPUT_DIR, "uspto.jsonl.gz")
