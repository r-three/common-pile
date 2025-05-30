#!/usr/bin/env sh

data_dir=${1:-"data"}
data_dir=${data_dir%/}

for file in ${data_dir}/dump/*.7z; do
    7z x -o${file%.7z} "${file}"
done
