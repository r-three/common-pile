#!/usr/bin/env bash

# Stackoverflow is larger than the other sites so they distribute each .xml file
# as its own .7z compressed file.
mkdir -p data/dump/stackoverflow.com

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
    wget -c -nc -P data/dump/stackoverflow.com --show-progress "${url}"
    if [[ ! -f "data/dump/stackoverflow.com/${file}.xml" ]]; then
      7z x -odata/dump/stackoverflow.com/ data/dump/stackoverflow.com/"${url##*/}"
    fi
done
