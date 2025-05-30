"""Update Author Formatting."""

import argparse
import multiprocessing as mp
import re

from common_pile import logs, utils
from common_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(description="Remove HTML from dolma documents.")
parser.add_argument(
    "--input",
    required=True,
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    required=True,
    help="The output version, this directory should be where the `documents` dir will live.",
)
parser.add_argument(
    "--filename",
    default="*.jsonl.gz",
    help="The filename to match with globs, probably needs to be escaped.",
)
# TODO: Respect this flag
parser.add_argument(
    "--overwrite",
    action="store_true",
    help="Should we overwrite previously processed examples?",
)
parser.add_argument(
    "--debug",
    action="store_true",
    help="Should we log when documents are not changed by preprocessing.",
)
parser.add_argument(
    "--processes",
    type=int,
    default=mp.cpu_count(),
    help="Number of processors for multicore.",
)
parser.add_argument("--meta", help="Location to save dolma processing metadata.")

logs.configure_logging()


class AuthorRenameParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        authors = example["metadata"].get("authors", [])
        new_authors = []
        for author in authors:
            if not isinstance(author, list):
                author = [author, ""]
            author = [str(a) for a in author]
            new_authors.append(author)
        example["metadata"]["authors"] = new_authors
        return example


def main(args):
    with utils.maybe_temp_dir(args.meta) as meta_dir:
        processor = AuthorRenameParallel(
            source_prefix=utils.dolma_input(args.input, args.filename),
            destination_prefix=utils.dolma_output(args.output),
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor(debug=args.debug, overwrite=args.overwrite)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
