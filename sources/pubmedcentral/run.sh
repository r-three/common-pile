#!/usr/bash/env sh

TOTAL_DOCS="${1:-0}"

# downloads the list of articles and keeps only permissively licensed articles
bash get-filelist.sh

# if total_docs is not 0, then only write TOTAL_DOCS lines into data/permissive_filelist.txt
if [ "${TOTAL_DOCS}" -ne 0 ]; then
    # -c -1 is to remove the trailing newline
    head -n "$((${TOTAL_DOCS}+1))" data/permissive_filelist.txt > data/permissive_filelist.txt.tmp

    mv data/permissive_filelist.txt.tmp data/permissive_filelist.txt
    echo "Reduced data/permissive_filelist.txt to ${TOTAL_DOCS} documents"
fi

# if the last line is empty, remove it
if [[ $(tail -c1 data/permissive_filelist.txt | wc -l) -gt 0 ]]; then
    truncate -s -1 data/permissive_filelist.txt
fi

echo "Downloading and converting to markdown..."
bash download-convert-to-md.sh
echo "Finished downloading and converting to markdown"

echo "Converting to dolma format..."
python to-dolma.py
echo "Finished converting to dolma format"
