"""Tool to recombine many small dolma files into fewer larger ones.

The tool tracks which example go into which shard to allow to the
alignment of shards when multiple versions need to be combined.
"""

import argparse
import contextlib
import copy
import glob
import json
import os
from typing import Dict, List

import contextual_logger
import smart_open

from licensed_pile import utils
from licensed_pile.logs import configure_logging, get_logger
from licensed_pile.write import shard_name

parser = argparse.ArgumentParser(
    description="Combine many dolma files into one. "
    "It also tracks where data came from to create "
    "aligned shards when combining different versions."
)
parser.add_argument(
    "--input", help="Where the dolma files live. A directory", required=True
)
parser.add_argument(
    "--output",
    help="Where the combined dolma files will live. A directory",
    required=True,
)
# We can't peek at one of the current files names as it will be an iterator, also
# if we are combining files, they might have different names.
# When we are making shards for later versions based on the mapping from the first
# the filename is fixed anyway.
parser.add_argument("--filename", help="The name to give the combined shards.")
parser.add_argument(
    "--shard_size", type=int, default=1, help="The size each combined shard will be."
)
parser.add_argument(
    "--shard_to_files", help="A path to a shard -> source file mapping."
)
parser.add_argument(
    "--shard_to_first_id", help="A path to a shard -> starting id mapping."
)
parser.add_argument("--shard_to_last_id", help="A path to a shard -> final id mapping.")


def read_dolma_file(path):
    with smart_open.open(path) as f:
        yield from (json.loads(l) for l in f if l)


def combine_dolma_files(
    input_dir: str,
    output_dir: str,
    filename: str,
    shard_size: int = 1,
    quiet: bool = False,
):
    logger = get_logger()
    # Make sure the input_dir ends with documents
    input_dir = utils.dolma_output(input_dir)
    # Find all .jsonl.gz files under input_dir
    files = glob.iglob(os.path.join(input_dir, "**", "*.jsonl.gz"), recursive=True)
    # Make sure output_dir ends with /documents
    logger.info(
        "Combining dolma shards into larger files, writing results to %s", output_dir
    )
    output_dir = utils.dolma_output(output_dir)
    # Make sure the dir exists, the combining process removes any dir structure
    # from the input dir tree so we only need to make this file.
    os.makedirs(output_dir, exist_ok=True)

    shard_idx = 0
    size = 0
    max_bytes = shard_size * 1000 * 1000 * 1000

    # Convert shard n to -> 0000n_{filename}
    shard = shard_name(filename, shard_idx)

    # Track a mapping from output shard to input file
    shard_to_files = {}
    # Track the last example moved from the input file to the output shard
    shard_to_last_id = {}
    # Track the first example moved from the input file to the output shard
    shard_to_first_id = {}
    # A list of file contributing to the current shard
    active_files = []
    # The last example id we wrote into the output file.
    last_id = None
    first_id = None

    shard_file = os.path.join(output_dir, shard)
    with contextlib.ExitStack() as stack:
        wf = stack.enter_context(smart_open.open(shard_file, "w"))
        stack.enter_context(logger(shard=shard_file))
        for dolma_file in files:
            # Only save the part relative to the root, this lets us find this
            # input file in a new revision.
            rel_dolma = os.path.relpath(dolma_file, input_dir)
            logger.info(
                "Starting to copy examples from %s into %s", dolma_file, shard_file
            )
            # Read example (via iterator) so we don't have them all in memory
            # at once.
            for example in read_dolma_file(dolma_file):
                # Serialize the data
                data = json.dumps(example)
                # Check if the new data will go over the size limit.
                size += len(data)
                # We need to make a new shard.
                if size >= max_bytes:
                    logger.close()
                    # Close the last shard, note that the /current/ data is *not*
                    # part of the just closed shard.
                    wf.close()
                    # Record the files that went into this shard. They are a list
                    # as we want to combine them in the same order going forward.
                    shard_to_files[shard] = copy.deepcopy(active_files)
                    # Record the last example id that went into this shard. Note
                    # the last_id currently points to the /previous/ data item
                    # as we have not assigned to it based on data
                    shard_to_last_id[shard] = last_id
                    # Record the first example id that went into this shard.
                    shard_to_first_id[shard] = first_id
                    # Increment shard, create new name, path, open file etc.
                    logger.info(
                        "Shard %s made from %s up to %s", shard, active_files, last_id
                    )
                    shard_idx += 1
                    shard = shard_name(filename, shard_idx)
                    shard_file = os.path.join(output_dir, shard)
                    wf = stack.enter_context(smart_open.open(shard_file, "w"))
                    stack.enter_context(logger(shard=shard_file))
                    logger.info(
                        "Shard size exceeded, creating new shard at %s", shard_file
                    )
                    # Reset size checker
                    size = 0
                    # Reset the active files to be empty, as long as the next
                    # data item is written, the current file will get added to
                    # the list.
                    active_files = []
                    # Set this to None so that it can be re-set now that it is
                    # tracking for the new shard.
                    first_id = None
                    logger.info(
                        "Starting to copy examples from %s into %s",
                        dolma_file,
                        shard_file,
                    )
                # Write the data and update the last_id to point to this item,
                # which will become the previous item in the next iteration of
                # the loop
                wf.write(data + "\n")
                last_id = example["id"]
                # We only let the first id be written once per shard, by the
                # first example that was output.
                if first_id is None:
                    first_id = example["id"]
                # Only add the current file to the active for this shard list
                # if the current bit of data is actually written to it. By doing
                # this /after/ the data is written, we avoid having a false
                # positive where the first element of a file triggers a new shard.
                # If we saved ourselves to "active" when the file was opened, it
                # would look like we contributed to the current shard.
                #
                # We also make sure to only add ourselves to the list once.
                if not active_files or active_files[-1] != (rel_dolma):
                    active_files.append(rel_dolma)
        # We don't actually need this check as the final data write will always
        # be into a shard that hasn't saved it's active files yet (as the size
        # check/new shard is from before the writing to the file.)
        if active_files:
            shard_to_files[shard] = copy.deepcopy(active_files)
            # In this case, the last_id *is* pointing to the /current/ data item
            # this is ok as the last id is *inclusive*, so saving the current data
            # id will mean include everything up-to and including this item in
            # the current shard.
            shard_to_last_id[shard] = last_id
            # Save the first id too.
            shard_to_first_id[shard] = first_id
            logger.info("Shard %s made from %s up to %s", shard, active_files, last_id)
    return shard_to_files, shard_to_first_id, shard_to_last_id


def combine_dolma_with_shard_info(
    input_dir: str,
    output_dir: str,
    shard_to_files: Dict[str, List[str]],
    shard_to_first_id: Dict[str, str],
    shard_to_last_id: Dict[str, str],
):
    logger = get_logger()
    # Ensure both paths end with /documents
    input_dir = utils.dolma_output(input_dir)
    output_dir = utils.dolma_output(output_dir)
    # Make sure the dir exists, the combining process removes any dir structure
    # from the input dir tree so we only need to make this file.
    os.makedirs(output_dir, exist_ok=True)
    # Iterate though the output shards we should generate.
    for shard, files in shard_to_files.items():
        with logger(shard=shard):
            logger.info("Starting to populate shard")
            # Find the last id in the last file that we should write to this shard.
            last_id = shard_to_last_id[shard]
            # Find the first id in the first file that we should write to this shard.
            first_id = shard_to_first_id[shard]
            # Create the new shard.
            with smart_open.open(
                os.path.join(utils.dolma_output(output_dir), shard), "w"
            ) as wf:
                # Are we skipping through the starting examples because they
                # were in an earlier shard?
                skipping = True
                # Iterate through the files that contributed to this shard.
                for dolma_file in files:
                    with logger(source=dolma_file):
                        logger.info("Filling shard from new source.")
                        # Write each example to the shard
                        for example in read_dolma_file(
                            os.path.join(input_dir, dolma_file)
                        ):
                            if (eid := example["id"]) == first_id:
                                logger.info(
                                    "Found first id in the first source file, start to fill",
                                    extra={"first_id": first_id},
                                )
                                skipping = False
                            if skipping:
                                logger.debug(
                                    "Skipping example, it was in the last shard.",
                                    extra={"id": eid},
                                )
                                continue
                            wf.write(json.dumps(example) + "\n")
                            # If we are writing the final open file, stop after we write
                            # the example with the final id.
                            if dolma_file == files[-1] and eid == last_id:
                                logger.info(
                                    "Found last id in final source file, closing shard.",
                                    extra={"last_id": last_id},
                                )
                                break


def read_shard_file(path):
    logger = get_logger()
    logger.info("Reading shard creation map from %s", path)
    with open(path) as f:
        return json.load(f)


def write_shard_file(shard_map, path):
    logger = get_logger()
    logger.info("Saving shard creation map to %s", path)
    with open(path, "w") as wf:
        json.dump(shard_map, wf)


def main():
    args = parser.parse_args()
    configure_logging()
    logger = get_logger()

    if not (args.shard_to_files or args.shard_to_first_id or args.shard_to_last_id):
        if args.filename is None:
            raise ValueError(
                "--filename needs to be given when creating the first combined dolma files."
            )
        logger.info("Combining files into shards and tracking which go where.")
        shard_to_files, shard_to_first_id, shard_to_last_id = combine_dolma_files(
            args.input, args.output, args.filename, args.shard_size
        )
        logger.info("Created %d new larger shards", len(shard_to_files))
        logger.info(
            "Each shard if made of %.2f on average",
            sum(len(fs) for fs in shard_to_files.values()) / len(shard_to_files),
        )
        write_shard_file(shard_to_files, "shard_to_files.json")
        write_shard_file(shard_to_first_id, "shard_to_first_id.json")
        write_shard_file(shard_to_last_id, "shard_to_last_id.json")
    elif args.shard_to_files and args.shard_to_first_id and args.shard_to_last_id:
        logger.info("Combining files into shards based on a mapping.")
        shard_to_files = read_shard_file(args.shard_to_files)
        shard_to_first_id = read_shard_file(args.shard_to_first_id)
        shard_to_last_id = read_shard_file(args.shard_to_last_id)
        combine_dolma_with_shard_info(
            args.input, args.output, shard_to_files, shard_to_first_id, shard_to_last_id
        )
    else:
        raise ValueError(
            "Either all or none of --shard_to_files, --shard_to_first_id, and "
            f"--shard_to_last_id should be given, got --shard_to_files={args.shard_to_files}, "
            f"--shard_to_first_id={args.shard_to_first_id}, and --shard_to_last_id={args.shard_to_last_id}"
        )


if __name__ == "__main__":
    main()
