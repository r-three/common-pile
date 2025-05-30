import argparse
import os
import urllib

import pandas as pd
import requests
from tqdm import tqdm

parser = argparse.ArgumentParser(description="Download books from DOAB")
parser.add_argument("metadata", type=str, help="Path to the metadata file")
parser.add_argument("output", type=str, help="Path to the output directory")
args = parser.parse_args()

# Load the metadata file
metadata = pd.read_csv(args.metadata)
filtered_metadata = metadata[
    metadata["BITSTREAM License"]
    .fillna("")
    .str.contains(r"licenses/by/|licenses/by-sa/", regex=True)
]
filtered_metadata = filtered_metadata[
    filtered_metadata["dc.language"] == "English[eng]"
]

# Iterate over the metadata and download the books
n_bytes = 0
pbar = tqdm(filtered_metadata.iterrows(), total=filtered_metadata.shape[0])
for idx, row in pbar:
    url = row["BITSTREAM Download URL"]
    if pd.isna(url):
        continue
    try:
        parsed = urllib.parse.urlsplit(url)
        encoded_path = urllib.parse.quote(parsed.path)
        encoded_query = urllib.parse.quote(parsed.query, safe="=&")
        encoded_url = urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, encoded_path, encoded_query, parsed.fragment)
        )
        response = requests.get(encoded_url, timeout=30)
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        continue

    if response.status_code != 200:
        print(f"Failed to download {url}")
        continue

    output_path = os.path.join(args.output, row["id"][:2], row["id"] + ".pdf")
    if not os.path.exists(os.path.dirname(output_path)):
        os.makedirs(os.path.dirname(output_path))

    with open(output_path, "wb") as f:
        f.write(response.content)

    n_bytes += response.content.__sizeof__()
    pbar.set_description(f"Downloaded {n_bytes / 1e6:.2f} MB")
