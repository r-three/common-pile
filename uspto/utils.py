import multiprocessing
import re
from itertools import islice

import polars as pl
import pypandoc
from rich.progress import track


def batched(iterable, n):
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def parse_html(html_string: str) -> str:
    if not html_string:
        return ""
    text = pypandoc.convert_text(html_string, "plain", "html", extra_args=["--quiet"])
    return re.sub(r"(?<!\n)\n(?!\n)", "", text)


# from: https://stackoverflow.com/a/74749075/19355181
def parallel_apply(max_concurrency: int, column: pl.Series) -> pl.Series:
    if max_concurrency == 0:
        max_concurrency = None
    with multiprocessing.get_context("spawn").Pool(max_concurrency) as pool:
        return pl.Series(pool.imap(parse_html, track(column)))
