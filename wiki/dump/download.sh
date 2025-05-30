#!/usr/bin/env bash

DATE=${1}
data_dir=${2:-"data"}
data_dir=${data_dir%/}

if [ -z ${DATE} ]; then
    echo "usage: download.sh [date YYYYMMDD] data/" 2> /dev/null
    exit 1
fi

declare -a wikis=(
    wiki
    wikibooks
    wikinews
    wikiquote
    wikisource
    wikiversity
    wikivoyage
    wiktionary
)

mkdir -p "${data_dir}/dumps"

for wiki in ${wikis[@]}; do
    filename="en${wiki}-${DATE}-pages-meta-current.xml.bz2"
    url="https://dumps.wikimedia.org/en${wiki}/${DATE}/${filename}"
    # Use wget to avoid re-downloading and continue downloads.
    wget -nc -c ${url} -O "${data_dir}/dumps/${filename}"
    # bzip2 doesn't decompress if the output is already there, so we don't check
    bunzip2 -k "${data_dir}/dumps/${filename}"
done
