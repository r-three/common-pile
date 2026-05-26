#!/usr/bin/env sh

set -e

python bulk_download.py --download_old --download_manifest
extract.sh
unzip data/arxiv-metadata-oai-snapshot.json.zip
python to-dolma.py
python preprocess.py
