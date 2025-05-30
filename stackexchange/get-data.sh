#!/usr/bin/env sh

set -e

data_dir=${1:-"data"}
data_dir=${data_dir%/}

./get-dumps.sh ${data_dir}
./preprocess-sites.sh ${data_dir}
# process stack overflow
python preprocess.py --input ${data_dir}/dump/stackoverflow.com --output ${data_dir}/stackexchange/v0/stackoverflow.com --shelve
