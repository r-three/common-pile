import argparse
import gzip
import os
import shutil

from huggingface_hub import snapshot_download

from common_pile.logs import configure_logging, get_logger


def parse_args():
    parser = argparse.ArgumentParser(
        description="Data Provenance Hugging Face Data Downloader"
    )
    parser.add_argument(
        "--hf_dataset",
        default="DataProvenanceInitiative/common_pile_set",
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

    if args.top_dir in ("~/.cache/huggingface", "~"):
        raise ValueError(
            f"Directory '{args.top_dir}' is the default Hugging Face cache directory. "
            "Choose a different directory to avoid potential data loss."
        )

    hugging_dir = os.path.join(args.top_dir, ".huggingface")
    if os.path.exists(hugging_dir):
        raise ValueError(
            f"Directory '{args.top_dir}' contains a .huggingface folder. "
            "Choose a different directory to avoid potential data loss."
        )

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

    if os.path.exists(hugging_dir):
        shutil.rmtree(hugging_dir)

    # walk through the top directory from bottom to top
    for root, dirs, files in os.walk(args.top_dir, topdown=False):
        # identify the jsonl files in the subdirectories
        for file in files:
            # construct the root path i.e. top_dir/open_assistant_octopack/Open Assistant OctoPack-processed.jsonl
            source_path = os.path.join(root, file)
            # construct the top directory path to move the json files i.e. top_dir/Open Assistant OctoPack-processed.jsonl
            dest_path = os.path.join(args.top_dir, file)

            # check if a gzip file already exists
            if not file.endswith(".gz"):
                # open the source path to write the files to
                with open(source_path, "rb") as f_in:
                    # write the gzip jsonl files to the destination folder
                    with gzip.open(dest_path + ".gz", "wb") as f_out:
                        # copy the compressed files to the correct destination folder
                        shutil.copyfileobj(f_in, f_out)

            # remove the source folder after copying
            os.remove(source_path)

        # check if the root directory is not top directory
        if root != args.top_dir:
            try:
                # then remove the root directory i.e. top_dir/open_assistant_octopack
                os.rmdir(root)
            except OSError:
                pass

    logger.info(f"Saved to {args.top_dir}")


if __name__ == "__main__":
    args = parse_args()
    configure_logging()
    main(args)
