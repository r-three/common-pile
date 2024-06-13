#!/usr/bin/env bash

data_dir=${1:-"data"}
data_dir=${data_dir%/}

# Stackoverflow is larger than the other sites so they distribute each .xml file
# as its own .7z compressed file.
mkdir -p ${data_dir}/dump/stackoverflow.com

STACKOVERFLOW=(
  "https://archive.org/download/stackexchange/stackoverflow.com-Badges.7z"
  "https://archive.org/download/stackexchange/stackoverflow.com-Comments.7z"
  "https://archive.org/download/stackexchange/stackoverflow.com-PostHistory.7z"
  "https://archive.org/download/stackexchange/stackoverflow.com-PostLinks.7z"
  "https://archive.org/download/stackexchange/stackoverflow.com-Posts.7z"
  "https://archive.org/download/stackexchange/stackoverflow.com-Tags.7z"
  "https://archive.org/download/stackexchange/stackoverflow.com-Users.7z"
  "https://archive.org/download/stackexchange/stackoverflow.com-Votes.7z"
)
for url in "${STACKOVERFLOW[@]}"; do
    file="${url##*.com-}"
    file="${file%.7z}"
    wget -c -nc -P ${data_dir}/dump/stackoverflow.com --show-progress "${url}"
    if [[ ! -f "${data_dir}/dump/stackoverflow.com/${file}.xml" ]]; then
      7z x -o${data_dir}/dump/stackoverflow.com/ ${data_dir}/dump/stackoverflow.com/"${url##*/}"
    fi
done
