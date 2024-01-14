"""Preprocess the arxiv data."""

import argparse
import glob
import multiprocessing as mp
import os
import re
from tempfile import TemporaryDirectory

import pylatexenc.latex2text

from licensed_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(
    description="Preprocess raw arxiv in the dolma format."
)
parser.add_argument(
    "--input",
    default="data/arxiv/raw",
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    default="data/arxiv/v0",
    help="The output version, this directory should be where the `documents` dir will live.",
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


l2t_db = pylatexenc.latex2text.get_default_latex_context_db()
l2t_db.add_context_category(
    "overrides",
    prepend=True,
    macros=[
        pylatexenc.latex2text.MacroTextSpec("includegraphics"),
        pylatexenc.latex2text.MacroTextSpec("maketitle"),
    ],
    environments=[
        pylatexenc.latex2text.EnvironmentTextSpec("array"),
        pylatexenc.latex2text.EnvironmentTextSpec("pmatrix"),
        pylatexenc.latex2text.EnvironmentTextSpec("bmatrix"),
        pylatexenc.latex2text.EnvironmentTextSpec("smallmatrix"),
    ],
)


class ArxivParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        latex = example["text"]
        document = latex.split(r"\begin{document}")[1]
        # TODO: Add better processing for citations and section references.
        try:
            text = pylatexenc.latex2text.LatexNodes2Text(
                math_mode="verbatim", latex_context=l2t_db
            ).latex_to_text(document)
        except Exception as e:
            logger = cls.get_logger()
            logger.warning(f"Failed to parse latex for document {example['id']}")
            return None
        lines = [l.strip() for l in text.splitlines()]
        text = "\n".join(
            l for l in lines if l == "" or l.startswith("\\") or len(l.split()) > 1
        )
        text = re.sub("\n\n+", "\n\n", text)
        example["text"] = text
        return example


def main(args):
    with TemporaryDirectory() as tempdir:
        processors = ArxivParallel(
            source_prefix=os.path.join(args.input, "documents", "*_arxiv.jsonl.gz"),
            destination_prefix=os.path.join(args.output, "documents"),
            metadata_prefix=tempdir,
            num_processes=args.processes,
        )
        processors(debug=args.debug)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
