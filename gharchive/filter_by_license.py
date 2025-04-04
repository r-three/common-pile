"""Convert the Aggregated Threads to the dolma format in parallel skips license info for now."""

import argparse
import json
import multiprocessing as mp
from datetime import datetime
from typing import Sequence

import requests
import utils as gh_utils
from tqdm import tqdm

from licensed_pile import logs, utils
from licensed_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(
    description="Filter threads (dolma format) based on license info."
)
parser.add_argument("--input", required=True, help="The input dolma files.")
parser.add_argument(
    "--output_dir",
    default="data/gharchive/v0/documents/",
    help="Where to save the dolma formatted data.",
)
parser.add_argument(
    "--license_info", required=True, help="File full of license information."
)
parser.add_argument(
    "--filename", default="*.jsonl.gz*", help="The base filename for shards."
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


class LocalGitHubArchiveLicenseParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        logger = cls.get_logger()
        license_info = kwargs["license_info"]
        repo = example["metadata"]["repo"]
        with logger(repo=repo):
            if repo not in license_info:
                logger.warning("Not license info found, removing.")
                return None
            license = license_info[repo]
            if license.license_type != "permissive":
                logger.info("Repo not open source, removing.")
                return None
            example["metadata"]["license"] = ",".join(
                filter(lambda l: l, (l.license for l in license.licenses))
            )
            example["metadata"]["license_type"] = license.license_type
            example["metadata"]["license_source"] = ",".join(
                l.license_source for l in license.licenses
            )
        return example


def get_license_info(repo: str):
    return requests.post(f"http://localhost:8882/license", json={"repo": repo}).json()


class ServiceGitHubArchiveLicenseParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        logger = cls.get_logger()
        repo = example["metadata"]["repo"]
        with logger(repo=repo):
            license = get_license_info(repo)
            if not license:
                logger.warning("Not license info found, removing.")
                return None
            if license["license_type"] != "permissive":
                logger.info("Repo not open source, removing.")
                return None
            example["metadata"]["license"] = ",".join(
                filter(lambda l: l, (l["license"] for l in license["licenses"]))
            )
            example["metadata"]["license_type"] = license["license_type"]
            example["metadata"]["license_source"] = ",".join(
                l["license_source"] for l in license["licenses"]
            )
        return example


def load_license_info(path: str) -> dict[str, gh_utils.LicenseInfo]:
    logger = logs.get_logger()
    logger.info(f"Reading license info from {path}")
    license_info = {}
    with open(path) as f:
        for line in tqdm(f):
            if line:
                data = json.loads(line)
                license_info[data["repo"]] = gh_utils.LicenseInfo.from_json(
                    data["license"]
                )
    return license_info


def main(args):
    # license_info = load_license_info(args.license_info)

    with utils.maybe_temp_dir(args.meta) as meta_dir:
        processor = ServiceGitHubArchiveLicenseParallel(
            source_prefix=utils.dolma_input(args.input, args.filename),
            destination_prefix=utils.dolma_output(args.output_dir),
            metadata_prefix=meta_dir,
            num_processes=args.processes,
            process_single_kwargs={"debug": args.debug, "overwrite": args.overwrite},
        )
        processor()


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
