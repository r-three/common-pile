#!/usr/bin/env sh

set -e

python buld_download.py --download_old --download_manifest
extract.sh
unzip data/arxiv-metadata-oat-snapshot.json.zip
python to-dolma.py
python preprocess.py
