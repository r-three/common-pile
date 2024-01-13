#!/usr/bin/env sh

set -e

./get-dumps.sh
./preprocess-sites.sh
# process stack overflow
python preprocess.py --input data/dump/stackoverflow.com --output data/stack-exchange/v0/stackoverflow.com --shelve
