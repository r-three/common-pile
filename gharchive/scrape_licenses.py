"""Populate the license cache from our data."""

import argparse
import asyncio
import glob
import itertools
import json
import os
import shelve
import time
from datetime import datetime, timezone
from typing import Iterator, Optional, Sequence, Tuple, Union

import httpx
import requests
from dateutil.parser import parse
from ghapi.all import GhApi
from ghapi.core import (
    HTTP403ForbiddenError,
    HTTP404NotFoundError,
    HTTP429TooManyRequestsError,
    HTTP451LegalReasonsError,
)
from utils import LicenseInfo, LicenseSnapshot

from licensed_pile import logs

parser = argparse.ArgumentParser(description="Scrape Licenses.")
parser.add_argument(
    "--repo_list", help="Path to a file that has a list of repos (JSON).", required=True
)
parser.add_argument(
    "--batch_size",
    help="Batch requests using GraphQL API.",
    required=False,
    default=200,
    type=int,
)
parser.add_argument(
    "--concurrent_batches",
    help="Number of concurrent",
    required=False,
    default=2,
    type=int,
)


async def batch_main(args, logger, rate_limit, repos, license_cache) -> None:
    i = 0
    if args.batch_size > 1:
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(30.0),
            limits=httpx.Limits(
                max_keepalive_connections=args.concurrent_batches,
                max_connections=args.concurrent_batches,
            ),
            headers={
                "User-Agent": "GitHub-License-Checker",
                "Authorization": f"token {os.environ['GITHUB_TOKEN']}",
            },
        ) as client:
            while i < len(repos):
                # Check if we need to wait for rate limit reset
                if rate_limit and rate_limit["remaining"] < args.batch_size:
                    logger.info(
                        f"Rate limit is low, {rate_limit['remaining']}. Waiting until {rate_limit['resetAt']}."
                    )
                    reset_time = parse(rate_limit["resetAt"])
                    sleep_seconds = (
                        reset_time - datetime.now(timezone.utc)
                    ).total_seconds()
                    if sleep_seconds > 0:
                        await asyncio.sleep(sleep_seconds)

                # Create batches, up to concurrent_batches
                batches = []
                current_index = i

                while (
                    current_index < len(repos)
                    and len(batches) < args.concurrent_batches
                ):
                    batch_end = min(current_index + args.batch_size, len(repos))
                    batch = repos[current_index:batch_end]
                    batches.append(
                        batched_get_license_info(
                            batch,
                            license_cache,
                            client=client,
                            rate_limit=rate_limit,
                        )
                    )
                    current_index = batch_end

                # Wait for all batches to complete
                batch_results = await asyncio.gather(*batches)

                # Update rate limit from the last result
                for result in batch_results:
                    *_, batch_rate_limit = result
                    rate_limit = batch_rate_limit

                # Update index to the new position
                i = current_index


def get_license_info(
    repo: str,
    license_cache: dict,
    github_api: GhApi,
    **kwargs,
) -> Optional[LicenseInfo]:
    alpha = datetime(1900, 1, 1)
    omega = datetime(9999, 1, 1)
    logger = logs.get_logger()
    if repo in license_cache:
        logger.info("cache hit, reusing license info")
        return license_cache[repo]
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


def build_graphql_query(repos: list[str]) -> Tuple[list[str], dict]:
    query_parts = []
    index = {}
    for idx, repo in enumerate(repos):
        owner, name = repo.split("/")
        alias = f"repo{idx + 1}"
        index[alias] = repo
        query_parts.append(
            f"""
        {alias}: repository(owner: "{owner}", name: "{name}") {{
            name
            owner {{
                login
            }}
            licenseInfo {{
                spdxId
                name
                url
                key
                id
            }}
        }}
        """
        )
    query_parts.append(
        """
    rateLimit {
        cost
        remaining
        resetAt
    }
    """
    )
    return query_parts, index


def check_github_graphql_rate_limit():
    logger = logs.get_logger()
    query = """
    query {
      rateLimit {
        limit
        cost
        remaining
        resetAt
      }
    }
    """

    response = requests.post(
        "https://api.github.com/graphql",
        json={"query": query},
        headers={
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "Content-Type": "application/json",
        },
    )

    if response.status_code == 200:
        data = response.json()
        return data["data"]["rateLimit"]
    else:
        logger.error(f"Error: {response.status_code}, {response.text}")
        return None


async def batched_get_license_info(
    repos: list[str],
    license_cache,
    client=None,
    rate_limit: int = None,
    max_retries: int = 3,
    retry_delay: int = 2,
    timeout: int = 20,
    **kwargs,
) -> Optional[Tuple[list[LicenseInfo], dict, Union[dict, None]]]:
    logger = logs.get_logger()
    res = []
    if isinstance(repos, str):
        repos = [repos]
    alpha = datetime(1900, 1, 1)
    omega = datetime(9999, 1, 1)
    queries = [repo for repo in repos if repo not in license_cache]
    if not queries:
        logger.info("cache hit, reusing past license info")
        return [], {}, rate_limit
    queries_, index = build_graphql_query(queries)
    full_query = "query {" + "\n".join(queries_) + "\n}"
    retries = 0
    while retries <= max_retries:
        try:
            response_obj = await client.post(
                "https://api.github.com/graphql",
                json={"query": full_query},
            )
            response_obj.raise_for_status()
            response = response_obj.json()
            for r in response["data"]:
                if r == "rateLimit":
                    rate_limit = response["data"]["rateLimit"]
                else:
                    repo = response["data"][r]
                    if repo is None:
                        license_info = LicenseInfo(
                            licenses=[
                                LicenseSnapshot(
                                    license="unlicensed", start=alpha, end=omega
                                )
                            ],
                            license_type="restrictive",
                        )
                    else:
                        license = repo.get("licenseInfo", None)
                        if license:
                            license_info = (
                                LicenseInfo(
                                    licenses=[
                                        LicenseSnapshot(
                                            license=repo["licenseInfo"]["spdxId"],
                                            start=alpha,
                                            end=omega,
                                        )
                                    ]
                                )
                                if license.get("spdxId")
                                else LicenseInfo(
                                    licenses=[
                                        LicenseSnapshot(
                                            license="unlicensed", start=alpha, end=omega
                                        )
                                    ],
                                    license_type="restrictive",
                                )
                            )
                        else:
                            license_info = LicenseInfo(
                                licenses=[
                                    LicenseSnapshot(
                                        license="unlicensed", start=alpha, end=omega
                                    )
                                ],
                                license_type="restrictive",
                            )

                    license_cache[index[r]] = license_info
                    res.append(license_info)

            logger.info(
                f"Fetching license info for {len(queries)} repos. Cost: {response['data']['rateLimit']['cost']}. Remaining: {response['data']['rateLimit']['remaining']}."
            )
            return res, index, rate_limit

        except Exception as e:
            if retries == max_retries:
                logger.error(f"Max retries reached. Last error: {e}")
                raise e
            logger.info("Error. Retrying")
            retries += 1
            logger.warning(
                f"Error occurred: {e}. Retrying ({retries}/{max_retries}) in {retry_delay} seconds..."
            )
            await asyncio.sleep(retry_delay)


def main():
    args = parser.parse_args()
    api = GhApi()
    logger = logs.configure_logging(level="INFO")
    with open(args.repo_list) as f:
        repos = json.load(f)
    wait = 60
    with shelve.open(f"{args.repo_list}.licenses") as license_cache:
        i = 0
        # Initial rate limit check only needed for the first iteration
        rate_limit = check_github_graphql_rate_limit() if args.batch_size > 1 else None
        logger.info(f"Rate limit: {rate_limit}")
        if args.batch_size > 1:
            asyncio.run(batch_main(args, logger, rate_limit, repos, license_cache))
        else:
            while True:
                if i >= len(repos):
                    break
                while api.limit_rem == 0:
                    logger.info(f"Waiting as API quota is low, {api.limit_rem}.")
                    time.sleep(1)
                try:
                    with logger(repo=repos[i], i=i):
                        _ = get_license_info(repos[i], license_cache, api)
                        i += 1
                        wait = max(60, wait // 8)
                except Exception as e:
                    error = str(e)
                    if "API rate limit exceeded" in error:
                        wait = min(wait * 4, 60 * 60)
                        logger.info(f"API rate limit exceeded. Waiting {wait} seconds.")
                        time.sleep(wait)
                    else:
                        logger.exception(f"Failed to process {repos[i]}, skipping")
                        i += 1


if __name__ == "__main__":
    main()
