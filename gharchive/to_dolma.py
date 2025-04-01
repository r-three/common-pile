"""Convert the Aggregated Threads to the dolma format in parallel skips license info for now."""

import argparse
import multiprocessing as mp
from datetime import datetime
from typing import Sequence

import bs4
import smart_open
from markdown_it import MarkdownIt
from utils import LicenseInfo, LicenseSnapshot

from licensed_pile import logs, utils
from licensed_pile.write import ShardParallelProcessor

SOURCE_NAME = "gharchive-threads"
MD = MarkdownIt("gfm-like", {"breaks": True, "html": True})


parser = argparse.ArgumentParser(description="Convert threads to dolma in parallel.")
parser.add_argument(
    "--bq", required=True, help="Pattern that picks up to the bigquery exported files."
)
parser.add_argument(
    "--output_dir",
    default="data/gharchive/raw/documents/",
    help="Where to save the dolma formatted data.",
)
parser.add_argument(
    "--filename", default="gharchive.jsonl.gz", help="The base filename for shards."
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


def infer_source(thread_url: str) -> str:
    if "issues/" in thread_url:
        return "issue"
    return "pull-request"


def clean_text(text: str) -> str:
    # Simple markdown cleaning.
    try:
        return bs4.BeautifulSoup(MD.render(text), "html.parser").get_text()
    except:
        logger = logs.get_logger()
        logger.exception("Parsing failed.", extra={"text": text})
        return text


def format_dolma(
    thread,
    source_name: str = "gharchive",
    **kwargs,
):
    logger = logs.get_logger()
    with logger(
        id=thread["thread_id"],
        url=thread["thread_url"],
        repo=thread["repo_name"],
        thread_author=thread["thread_author_username"],
    ):
        # Format comments into thread while removing authors that are bots.
        text = [thread.get("thread_title", ""), thread.get("thread_body", "")]
        if "thread_author_username" in thread:
            authors = {thread["thread_author_username"]}

        for author, comment, ts in zip(
            thread["comment_author_username"],
            thread["comment_body"],
            thread["comment_timestamp"],
        ):
            text.append(comment)
            authors.add(author)

        text = map(clean_text, text)
        # Remove empty strings.
        text = filter(lambda t: t, text)
        text = "\n\n".join(text)

        if not text:
            logger.warning("Cleaning has reduced text to nothing, skipping...")
            return None

        metadata = format_metadata(thread, authors, "")
        return {
            "id": thread["thread_id"],
            "text": text,
            "source": f"{source_name}/{infer_source(thread['thread_url'])}",
            "created": datetime.fromisoformat(thread["thread_timestamp"]).isoformat(),
            "added": datetime.utcnow().isoformat(),
            "metadata": metadata,
        }


def format_metadata(thread, authors: Sequence[str], license: str = "") -> dict:
    return {
        "authors": sorted(authors),
        "repo": thread["repo_name"],
        "url": thread["thread_url"],
        "license": license,  # ToDo: Get license of repo.
    }


class GitHubArchiveThreadsParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        delimiter = kwargs.get("delimiter", "⇭⇭⇭")
        for key in (
            "comment_author_username",
            "comment_body",
            "comment_timestamp",
        ):
            example[key] = example.get(key, "").split(delimiter)
        return format_dolma(example)


def main(args):
    with utils.maybe_temp_dir(args.meta) as meta_dir:
        processor = GitHubArchiveThreadsParallel(
            source_prefix=args.bq,
            destination_prefix=utils.dolma_output(args.output_dir),
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor(debug=args.debug, overwrite=args.overwrite)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
