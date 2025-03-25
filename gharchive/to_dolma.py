"""Convert the Aggregated Threads to the dolma format."""

import argparse
import dataclasses
import functools
import glob
import itertools
import json
import os
import re
import shelve
from datetime import datetime
from typing import Iterator, Optional, Sequence

import bs4
import smart_open
from ghapi.all import GhApi
from ghapi.core import (
    HTTP403ForbiddenError,
    HTTP404NotFoundError,
    HTTP429TooManyRequestsError,
    HTTP451LegalReasonsError,
)
from markdown_it import MarkdownIt

from licensed_pile import logs
from licensed_pile.licenses import PermissiveLicenses
from licensed_pile.write import to_dolma

SOURCE_NAME = "gharchive-threads"
MD = MarkdownIt("gfm-like", {"breaks": True, "html": True})

parser = argparse.ArgumentParser(description="Convert threads to dolma.")
parser.add_argument(
    "--bq", required=True, help="Pattern that picks up to the bigquery exported files."
)
parser.add_argument(
    "--keep_bots",
    action="store_true",
    help="Should we keep comments/threads from bots?",
)
parser.add_argument(
    "--license_cache", default="data/license_cache", help="Where to cache license data."
)
parser.add_argument(
    "--output_dir",
    default="data/gharchive/raw/documents/",
    help="Where to save the dolma formatted data.",
)
parser.add_argument(
    "--filename", default="gharchive.jsonl.gz", help="The base filename for shards."
)
parser.add_argument(
    "--shard_size", type=int, default=1, help="Size, in GB, for each shard."
)
parser.add_argument(
    "--fetch_license",
    action="store_true",
    help="Should you try to get license infomation from github when it is missing?",
)


ALLOWED_LICENSES = frozenset(
    {
        "MIT",
    }
)


@dataclasses.dataclass
class LicenseSnapshot:
    license: str
    start: datetime
    end: datetime

    def active(self, time):
        return self.start <= time <= self.end


@dataclasses.dataclass
class LicenseInfo:
    licenses: list[LicenseSnapshot]
    license_type: str = ""

    def license(self, time):
        for l in self.licenses:
            if l.active(time):
                return True
        return False


def get_license_info(
    repo: str,
    license_cache: dict,
    github_api: GhApi,
    fetch_license: bool = False,
    **kwargs,
) -> Optional[LicenseInfo]:
    alpha = datetime(1900, 1, 1)
    omega = datetime(9999, 1, 1)
    logger = logs.get_logger()
    if repo in license_cache:
        logger.info("cache hit, reusing license info")
        return license_cache[repo]
    if not fetch_license:
        logger.debug(f"license not found and --fetch_license not set, skipping.")
        return None
    owner, repo_name = repo.split("/")
    logger.info("cache miss, fetching license info")
    try:
        resp = github_api.licenses.get_for_repo(owner, repo_name)
        license_info = LicenseInfo(
            licenses=[
                LicenseSnapshot(
                    license=resp["license"]["spdx_id"], start=alpha, end=omega
                )
            ]
        )
    except HTTP404NotFoundError:
        license_info = LicenseInfo(
            licenses=[LicenseSnapshot(license="unlicensed", start=alpha, end=omega)],
            license_type="restrictive",
        )
    except HTTP403ForbiddenError as e:
        error = json.loads(re.sub(r".*=*Error Body=*", "", e.msg, flags=re.DOTALL))
        reason = error.get("block", {}).get("reason")
        if reason == "tos":
            license_info = LicenseInfo(
                licenses=[
                    LicenseSnapshot(license="tos-violation", start=alpha, end=omega)
                ],
                license_type="restrictive",
            )
        elif reason == "sensitive_data":
            license_info = LicenseInfo(
                licenses=[
                    LicenseSnapshot(license="sensitive-data", start=alpha, end=omega)
                ],
                license_type="restrictive",
            )
        elif reason == "private_information":
            license_info = LicenseInfo(
                licenses=[
                    LicenseSnapshot(
                        license="private-information", start=alpha, end=omega
                    )
                ],
                license_type="restrictive",
            )
        else:
            logger.exception("error when fetching repo information.")
            raise
    except HTTP451LegalReasonsError:
        license_info = LicenseInfo(
            licenses=[LicenseSnapshot(license="dmca", start=alpha, end=omega)],
            license_type="restrictive",
        )
    license_cache[repo] = license_info
    return license_info


def license_check(license_info, time, allowed_licenses: set[str]) -> bool:
    return license_info.license(time) in allowed_licenses


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
        return ""


def format_dolma(
    thread,
    license_cache: dict,
    github_api: GhApi,
    source_name: str = "gharchive",
    keep_bots: bool = False,
    allowed_licenses: set[str] = ALLOWED_LICENSES,
    fetch_license: bool = False,
    **kwargs,
):
    logger = logs.get_logger()
    with logger(
        id=thread["thread_id"],
        url=thread["thread_url"],
        repo=thread["repo_name"],
        thread_author=thread["thread_author_username"],
    ):
        # license_info = get_license_info(thread["repo_name"], license_cache, github_api, fetch_license)
        # if license_info is None:
        #     logger.warning("License not found, skipping.")
        #     return None

        # Format comments into thread while removing authors that are bots.
        text = [thread["thread_title"], thread["thread_body"]]
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

        # TODO: update licenses
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


def read_threads(path: str, delimiter: str = "⇭⇭⇭") -> Iterator[dict]:
    logger = logs.get_logger()
    with logger(file=path):
        logger.info(f"Reading from file {path}")
        with smart_open.open(path, compression=".gz") as f:
            for i, line in enumerate(f):
                logger.debug("Processing Line", extra={"line": i})
                if line:
                    data = json.loads(line)
                    # Aggregated columns are joined with delimiter as the overhead
                    # from storing it as an array was too large.
                    for key in (
                        "comment_author_username",
                        "comment_body",
                        "comment_timestamp",
                    ):
                        data[key] = [
                            json.loads(c) for c in data.get(key, "").split(delimiter)
                        ]
                    yield data


def main():
    args = parser.parse_args()
    api = GhApi()
    logger = logs.configure_logging(level="INFO")
    logger.info(f"Reading data from shards according to {args.bq}")
    files = glob.iglob(args.bq)
    threads = itertools.chain(*map(read_threads, files))
    # Cache results of hitting the github api for license info across runs
    with shelve.open(args.license_cache) as license_cache:
        threads = map(
            functools.partial(
                format_dolma,
                keep_bots=args.keep_bots,
                license_cache=license_cache,
                github_api=api,
                fetch_license=args.fetch_license,
            ),
            threads,
        )
        threads = filter(lambda t: t is not None, threads)
        to_dolma(threads, args.output_dir, args.filename, args.shard_size)


if __name__ == "__main__":
    main()
