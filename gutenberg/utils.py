"""Tools for working with PG data."""

import os
import urllib.parse


def parse_id(metadata):
    metadata["id"] = os.path.basename(urllib.parse.urlparse(metadata["id"]).path)
    return metadata


FILE_ORDERING = {
    "text/plain": 0,
    "text/plain; charset=utf-8": 1,
    "text/plain; charset=us-ascii": 2,
    "text/plain; charset=iso-8859-1": 3,
}


def file_type(results):
    if results:
        results = sorted(results, key=lambda x: FILE_ORDERING[str(x["format"])])
        return results[0:1]
    return results
