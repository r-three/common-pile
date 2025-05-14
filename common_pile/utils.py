"""Shared utilities like string processing."""

import glob
import os
import re
from contextlib import contextmanager
from tempfile import TemporaryDirectory
from typing import Optional


# We don't use snake case as the string methods added in PIP616 are named like this.
def removeprefix(s: str, prefix: str) -> str:
    """In case we aren't using python >= 3.9"""
    if s.startswith(prefix):
        return s[len(prefix) :]
    return s[:]


# We don't use snake case as the string methods added in PIP616 are named like this.
def removesuffix(s: str, suffix: str) -> str:
    """In case we aren't using python >= 3.9"""
    # Check for suffix to avoid calling s[:-0] for an empty string.
    if suffix and s.endswith(suffix):
        return s[: -len(suffix)]
    return s[:]


def dolma_input(input_path: str, filepattern: str = "*.jsonl.gz") -> str:
    # If the input is directly to a file, or it is a glob that returns matches,
    # use as is.
    if (
        (os.path.exists(input_path) and os.path.isfile(input_path))
        or not os.path.isdir(input_path)
        and glob.glob(input_path, recursive=True)
    ):
        return input_path
    # Otherwise it is probably meant as a directory, so add the ../documents/${filepattern}
    # for ease of use.
    if filepattern is None:
        raise ValueError(
            "filepattern must be provided when input_path isn't a file/matched glob."
        )
    return os.path.join(input_path, "documents", filepattern)


def dolma_output(output_path: str):
    # Make sure the output ends in .../documents, many people forget this.
    if re.match(".*/documents/?$", output_path):
        return output_path
    return os.path.join(output_path, "documents")


@contextmanager
def maybe_temp_dir(path: Optional[str] = None):
    if path is not None:
        yield path
    else:
        with TemporaryDirectory() as tmpdir:
            yield tmpdir
