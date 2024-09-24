"""Shared Utilities related to scraping."""

import logging
from typing import Dict, Optional

import requests
from tenacity import retry, stop_after_attempt, wait_random_exponential

# A user agent that says we are compatible with most websites (most browsers
# start with Mozilla/5.0) and also tells that we are a bot and includes a link
# for context on why we are scraping. We hope this fosters good will with site
# owners.
USER_AGENT = "Mozilla/5.0 (compatible; Licensed-Pile-bot/0.1; +http://www.github.com/r-three/licensed-pile)"

DEFAULT_HEADERS = {"User-Agent": USER_AGENT}


@retry(stop=stop_after_attempt(5), wait=wait_random_exponential(multiplier=1, max=30))
def get_page(
    url: str,
    params: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
):
    """GET page with retries, uses our licensed-pile default user-agent string."""
    params = params if params is not None else {}
    headers = headers if headers is not None else {}
    # Unpack the defaults first so the user provided ones can override them.
    headers = {**DEFAULT_HEADERS, **headers}
    resp = requests.get(url, params=params, headers=headers)
    logging.debug(f"Sending GET to {resp.url}")
    if resp.status_code != 200:
        # TODO: Update logger
        logging.warning(
            f"Failed request to {resp.url}: {resp.status_code}, {resp.reason}"
        )
        raise RuntimeError(f"Failed request to {resp.url}")
    return resp
