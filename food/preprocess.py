"""Parse the webpages using dolma tooling."""


import argparse
import datetime
import glob
import multiprocessing as mp
import os
import re
import textwrap
import urllib.parse
from tempfile import TemporaryDirectory

import bs4
import tqdm

from common_pile import logs, utils
from common_pile.write import ShardParallelProcessor

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
parser.add_argument(
    "--meta",
    help="Location to store Dolma Metadata information.",
)

# Dolma later sets the log level to error, need to override cls.get_logger() if
# we want to see info methods.
logs.configure_logging()


class FoodistaParallel(ShardParallelProcessor):
    @classmethod
    def process_example(cls, example, **kwargs):
        logger = cls.get_logger()

        # Filter out static, siteinfo pages as they are varied and have little
        # content.
        if urllib.parse.urlparse(example["metadata"]["url"]).path.startswith(
            "/static/"
        ):
            return None

        html = example["text"]

        with logger(id=example["id"]):
            text, authors, date = parse_page(html)
            if text is None:
                return None

        example["text"] = text
        example["created"] = date
        example["metadata"]["authors"] = authors

        return example


def clean_author(author: str) -> str:
    author = re.sub("^(Creator|Author):?", "", author.strip())
    return author.strip()


def clean_date(date: str) -> str:
    date = re.sub(r"(Added):?", "", date.strip())
    return date.strip()


def clean_text(html) -> str:
    for step in html.find_all("div", class_=("step-number")):
        if step.string == "1":
            step.string = f"{step.string} "
        else:
            step.string = f" {step.string} "
    text = html.get_text().strip()
    # Remove when the text is only the section header.
    if text in ("Tools", "Ingredients", "Preparation", "About", "Information"):
        return ""
    return text


def parse_page(html, include_user_id: bool = False):
    """Convert a page's html to plain text for LLM training."""
    logger = logs.get_logger()
    soup = bs4.BeautifulSoup(html, "html.parser")

    result = []
    authors = []
    # Find the title of the article
    if title := soup.find("h1", id="page-title"):
        # .get_text() seems to be smart enough to get the text from the inner <a>...</a>
        title = title.get_text().strip()
        result.append(title)
    else:
        logger.warning("Failed to find title.")

    # Find the author's name (which is included in the text) and the user id (which
    # will be included in the metadata).
    if author := soup.find("div", class_="pane-node-author"):
        if user_id := author.find("a", class_="username"):
            user_id = user_id.get("href")
        else:
            logger.warning("Failed to find the user_id for the author.")
        author = clean_author(author.get_text()).strip()
        result.append(f"By: {author}")
        if include_user_id:
            authors.append((author, user_id))
        else:
            authors.append(author)
    else:
        logger.warning("Failed to find author.")

    # Find the date it was published.
    if date := soup.find("div", class_="pane-node-created"):
        date = clean_date(date.get_text()).strip()
        result.append(f"Published: {date}")
    else:
        logger.warning("Failed to find date.")

    # Find the text of the page.
    if text_tags := soup.find_all(
        "div",
        class_=(
            "pane-node-body",
            "pane-node-field-rec-ing",
            "pane-node-field-rec-steps",
            "pane-node-field-rec-tools",
            "pane-node-field-about",
        ),
    ):
        text = []
        # Create an empty line between the header and the body text.
        for t in text_tags:
            t = clean_text(t).strip()
            if t:
                text.append(f"\n{t}")
        if not text:
            logger.warning(f"Cleaned text was empty.")
            return None, None, None
        result.extend(text)
    else:
        logger.warning("Failed to find text for example.")

    # Collect all comments first as we may filter them out later.
    comments = []
    # Answers on /question/ pages are saved as comments. We check what the header
    # is decide if it should be "Comments" or "Answers" in the output text.
    comment_header = "Comments"
    if comment_title := soup.find("div", class_="pane-node-comments"):
        if title := comment_title.find("h2", class_="pane-title"):
            if title.get_text().strip() == "Answers":
                comment_header = "Answers"
    # Find possible comments on the page.
    if comments_soup := soup.find_all("div", class_="comment"):
        # Parse the author, date, and text, from each comment
        for comment in comments_soup:
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
                else:
                    logger.warning(f"Failed to find comment author.")
                # The date follows a <br> which bs4 wraps in <br>...</br>
                if comment_date := comment_submitted.find("br"):
                    comment_date = comment_date.get_text().strip()
                else:
                    logger.warning(f"Failed to find comment date.")
            if comment_text := comment.find("div", class_="content"):
                comment_text = comment_text.get_text().strip()
            else:
                logger.warning(f"Failed to find comment text.")

            # Some comments seems to be snippets from other sites, ignore those
            if comment_text.startswith("[...]") or comment_text.endswith("[...]"):
                continue
            comments.append(
                {
                    "author": comment_author,
                    "date": comment_date,
                    "text": comment_text,
                    "user_id": user_id,
                }
            )
    else:
        # Not all articles have comments, so we don't call this an error.
        logger.info(f"Didn't find comments.")

    # Add non-filtered comments into the text.
    if comments:
        result.append(f"\n{comment_header}:")
        for comment in comments:
            if include_user_id:
                authors.append((comment["author"], comment["user_id"]))
            else:
                authors.append(comment["author"])
            if comment["author"]:
                result.append(comment["author"])
            if comment["date"]:
                result.append(comment["date"])
            if comment["text"]:
                result.append(f"\n{comment['text']}")

    result = "\n".join(result).strip()
    return result, sorted(set(authors)), parse_date(date)


def parse_date(date: str) -> datetime.datetime:
    """Parse a date into a datetime object."""
    date_formats = ("%B %d, %Y", "%b %d, %Y", "%A, %B %d, %Y - %I:%M%p")
    for fmt in date_formats:
        try:
            return datetime.datetime.strptime(date, fmt).isoformat()
        except:
            pass
    logger = logs.get_logger()
    logger.warning(f"Filed to parse date: {date}")
    return date


def main(args):
    with utils.maybe_temp_dir(path=args.meta) as meta_dir:
        processor = FoodistaParallel(
            source_prefix=utils.dolma_input(args.input, "*.jsonl.gz"),
            destination_prefix=utils.dolma_output(args.output),
            metadata_prefix=meta_dir,
            num_processes=args.processes,
        )
        processor(debug=args.debug)


if __name__ == "__main__":
    # Dolma examples use spawn over fork, unsure why but lets follow them.
    mp.set_start_method("spawn")
    args = parser.parse_args()
    main(args)
