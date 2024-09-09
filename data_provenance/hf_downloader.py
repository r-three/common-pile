import argparse
import gzip
import os
import shutil

from huggingface_hub import snapshot_download

from licensed_pile.logs import configure_logging, get_logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Data Provenance Hugging Face Data Downloader"
    )
    parser.add_argument(
        "--hf_dataset",
        default="DataProvenanceInitiative/common_pile_ultra_permissive",
        help="The label for the HuggingFace dataset that can be used in HuggingFace's load_dataset()",
    )
    parser.add_argument(
        "--hf_token",
        default=None,
        help="Your API token for HuggingFace datasets",
    )
    parser.add_argument(
        "--top_dir", default="data/raw-data-provenance", help="Path to output directory"
    )
    return parser.parse_args()


def main(args):
    logger = get_logger()
    logger.info(f"Filtering to just the datasets in {args.hf_dataset}")

    os.makedirs(args.top_dir, exist_ok=True)

    logger.info(f"Downloading {args.hf_dataset}")

    snapshot_download(
        repo_id=args.hf_dataset,
        repo_type="dataset",
        local_dir=args.top_dir,
        allow_patterns="*.jsonl",
        token=args.hf_token,
    )

    logger.info(f"Saving {args.hf_dataset}")

    hugging_dir = os.path.join(args.top_dir, ".huggingface")
    if os.path.exists(hugging_dir):
        shutil.rmtree(hugging_dir)

    for root, dirs, files in os.walk(args.top_dir):
        for file in files:
            source_path = os.path.join(root, file)
            dest_path = os.path.join(args.top_dir, file)

            while os.path.exists(dest_path) or os.path.exists(dest_path + ".gz"):
                name, ext = os.path.splitext(file)
                dest_path = os.path.join(args.top_dir, f"{name}_{ext}")

            with open(source_path, "rb") as f_in:
                with gzip.open(dest_path + ".gz", "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)

            os.remove(source_path)

        if root != args.top_dir:
            try:
                os.rmdir(root)
            except OSError:
                pass

    logger.info(f"Saved to {args.top_dir}")


if __name__ == "__main__":
    args = parse_args()
    configure_logging()
    main(args)
