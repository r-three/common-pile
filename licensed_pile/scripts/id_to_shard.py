#!/usr/bin/env python3

import argparse
import glob
import json
import multiprocessing as mp
import os
import re
from tempfile import TemporaryDirectory

import smart_open

from licensed_pile import utils, write


class IdToShardParallel(write.ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        return {"id": example["id"]}


def main():
    mp.set_start_method("spawn")
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--input", help="", required=True)
    parser.add_argument(
        "--output",
        help="",
        default="id_to_shards.json",
    )
    parser.add_argument("--processes", type=int, default=mp.cpu_count(), help="")
    args = parser.parse_args()

    args.input = utils.dolma_input(args.input)

    with TemporaryDirectory() as tempdir:
        processor = IdToShardParallel(
            source_prefix=args.input,
            destination_prefix=tempdir,
            metadata_prefix=tempdir,
            num_processes=args.processes,
        )
        processor()

        id_to_shard = {}
        for shard_file in glob.iglob(
            os.path.join(tempdir, os.path.basename(args.input))
        ):
            if shard := re.search("^(\d{5})_", os.path.basename(shard_file)):
                shard = shard.group(1)
                with smart_open.smart_open(shard_file) as f:
                    ids = [json.loads(l)["id"] for l in f if l]
                    id_to_shard |= dict.fromkeys(ids, shard)
        with smart_open.smart_open(args.output, "w") as wf:
            json.dump(id_to_shard, wf)


if __name__ == "__main__":
    main()
