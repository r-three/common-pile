#!/usr/bin/env bash

DATE=${1}
export_dir=${2:-"data/dumps"}
export_dir=${export_dir%/}
output_dir=${3:-"../data/wiki/dump/raw"}
output_dir=${output_dir%/}

if [ -z ${DATE} ]; then
    echo "usage: to_dolma.sh [date YYYYMMDD] dump/ data/wiki/raw/documents" 2> /dev/null
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

for wiki in ${wikis[@]}; do
    filename="en${wiki}-${DATE}-pages-meta-current.xml"
    # Check for output
    if [[ ${wiki} == "wiki" ]]; then
        url="https://wikipedia.com"
    else
        url="https://${wiki}.com"
    fi
    python ../to_dolma.py --license CC-BY-SA/4.0 --wiki "${url}" --export "${export_dir}/${filename}" --output_dir "${output_dir}" --last_author --source "wiki/dump"
done
