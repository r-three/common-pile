#!/usr/bin/env python3

import argparse
import multiprocessing as mp

from licensed_pile import logs, utils
from licensed_pile.write import ShardParallelProcessor

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
parser.add_argument(
    "--features_to_keep",
    action="append",
    help="The list of features to keep in the resulting dataset.",
)
parser.add_argument("--meta", help="Location to save dolma processing metadata.")

logs.configure_logging(level="INFO")


class RemoveFeaturesParallel(ShardParallelProcessor):
    @classmethod
    def process_example(
        cls, example, features_to_keep: set[str] = frozenset(("text",)), **kwargs
    ):
        logger = cls.get_logger()
        return {k: v for k, v in example.items() if k in features_to_keep}


def main(args):
    features_to_keep = (
        set(args.features_to_keep) if args.features_to_keep is not None else {"text"}
    )
    logger = logs.get_logger()
    source_prefix = utils.dolma_input(args.input, args.filename)
    destination_prefix = utils.dolma_output(args.output)
    logger.info(
        f"Keeping {features_to_keep} from {source_prefix} and saving to {destination_prefix}"
    )
    with utils.maybe_temp_dir(args.meta) as meta_dir:
        processor = RemoveFeaturesParallel(
            source_prefix=source_prefix,
            destination_prefix=destination_prefix,
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor(
            debug=args.debug,
            overwrite=args.overwrite,
            features_to_keep=features_to_keep,
        )


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
