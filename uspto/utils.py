import multiprocessing
import re
from functools import partial
from itertools import islice

import polars as pl
import pypandoc
from rich.progress import track


def batched(iterable, n):
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch


def parse_html(claims: bool, html_string: str) -> str:
    if not html_string:
        return ""
    text = pypandoc.convert_text(html_string, "plain", "html", extra_args=["--quiet"])
    # remove single newlines that are not surrounded by other newlines as those are likely line length formatting.
    new_line_pattern = r"(?<!\n)\n(?!\n)"
    # also add line-breaks after <number><periods> for claims (as they are all numbered).
    list_pattern = r"(\s\d+\.\s)"
    text = re.sub(new_line_pattern, "", text)
    if claims:
        text = re.sub(list_pattern, r"\n\1", text)
    return text


# from: https://stackoverflow.com/a/74749075/19355181
def parallel_apply(claims: bool, max_concurrency: int, column: pl.Series) -> pl.Series:
    if claims:
        fn = partial(parse_html, True)
    else:
        fn = partial(parse_html, False)
    if max_concurrency == 0:
        max_concurrency = None
    # polars mainly handles the concurrency but the pandoc calls add as a blocker. This is a workaround to
    # increase the concurrency of the pandoc calls.
    with multiprocessing.get_context("spawn").Pool(max_concurrency) as pool:
        return pl.Series(pool.imap(fn, track(column, description="Processing column")))
