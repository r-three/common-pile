"""Parse the webpages using dolma tooling."""


import argparse
import datetime
import glob
import multiprocessing as mp
import os
import textwrap
from tempfile import TemporaryDirectory

import bs4
import tqdm

from licensed_pile import logs
from licensed_pile.write import ShardParallelProcessor

parser = argparse.ArgumentParser(
    description="Preprocess raw pages in the dolma format."
)
parser.add_argument(
    "--input",
    default="data/foodista/raw",
    help="The input version, this directory should be where the `documents` dir lives.",
)
parser.add_argument(
    "--output",
    default="data/foodista/v0",
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

# Dolma later sets the log level to error, need to override cls.get_logger() if
# we want to see info methods.
logs.configure_logging("dolma.FoodistaParallel")


class FoodistaParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        logger = cls.get_logger()

        html = example["text"]

        text, authors, date = parse_page(html, example["id"])

        example["text"] = text
        example["created"] = date
        example["metadata"]["authors"] = authors

        return example


def parse_page(html, idx):
    """Convert a page's html to plain text for LLM training."""
    logger = logs.get_logger("dolma.FoodistaParallel")
    soup = bs4.BeautifulSoup(html, "html.parser")

    result = []
    authors = []
    # Find the title of the article
    if title := soup.find("h1", id="page-title"):
        # .get_text() seems to be smart enough to get the text from the inner <a>...</a>
        title = title.get_text().strip()
        result.append(title)
    else:
        logger.warning(f"Failed to find title for example: {idx}")

    # Find the author's name (which is included in the text) and the user id (which
    # will be included in the metadata).
    if author := soup.find("div", class_="pane-node-author"):
        if user_id := author.find("a", class_="username"):
            user_id = user_id.get("href")
        else:
            logger.warning(
                f"Failed to find the user_id for the author in example: {idx}"
            )
        author = author.get_text().strip()
        result.append(f"By: {author}")
        authors.append((author, user_id))
    else:
        logger.warning(f"Failed to find author for example: {idx}")

    # Find the date it was published.
    if date := soup.find("div", class_="pane-node-created"):
        date = date.get_text().strip()
        result.append(f"Published: {date}")
    else:
        logger.warning(f"Failed to find date for example: {idx}")

    # TODO: split into blocks with extra new lines?
    # Find the text of the page.
    if text := soup.find("div", class_="pane-node-body"):
        text = text.get_text().strip()
        # Create an empty line between the header and the body text.
        result.append(f"\n{text}")
    else:
        logger.warning(f"Failed to find text for example: {idx}")

    # Find possible comments on the page.
    if comments := soup.find_all("div", class_="comment"):
        result.append("\nComments:")
        # Parse the author, date, and text from each comment
        for comment in comments:
            result.append("\n")
            if comment_submitted := comment.find("div", class_="submitted"):
                if comment_author := comment_submitted.find(
                    ["span", "a"], class_="username"
                ):
                    user_id = ""
                    # If someone with a user account comments, their author section
                    # is a link instead of a span.
                    if comment_author.name == "a":
                        user_id = comment_author["href"]
                    comment_author = comment_author.get_text().strip()
                    result.append(comment_author)
                    authors.append((comment_author, user_id))
                else:
                    logger.warning(f"Failed to find comment author in example: {idx}")
                # The date follows a <br> which bs4 wraps in <br>...</br>
                if comment_date := comment_submitted.find("br"):
                    comment_date = comment_date.get_text().strip()
                    result.append(comment_date)
                else:
                    logger.warning(f"Failed to find comment date in example: {idx}")
            if comment_text := comment.find("div", class_="content"):
                comment_text = comment_text.get_text().strip()
                result.append(f"\n{comment_text}")
            else:
                logger.warning(f"Failed to find comment text in example: {idx}")
    else:
        # Not all articles have comments, so we don't call this an error.
        logger.info(f"Didn't find comments for example: {idx}")

    result = "\n".join(result).strip()
    return result, sorted(set(authors)), parse_date(date)


def parse_date(date: str) -> datetime.datetime:
    """Parse a date into a datetime object."""
    date_formats = ["%B %d, %Y", "%b %d, %Y"]
    for fmt in date_formats:
        try:
            return datetime.datetime.strptime(date, fmt).isoformat()
        except:
            pass
    logger = logs.get_logger("food")
    logger.warning(f"Filed to parse date: {date}")
    return date


def main(args):
    with TemporaryDirectory() as tempdir:
        processor = FoodistaParallel(
            source_prefix=os.path.join(args.input, "documents", "*.jsonl.gz"),
            destination_prefix=os.path.join(args.output, "documents"),
            metadata_prefix=tempdir,
            num_processes=args.processes,
        )
        processor(debug=args.debug)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
