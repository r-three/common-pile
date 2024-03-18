#!/usr/bash/env sh

TOTAL_DOCS="${1:-0}"

# downloads each file from the filelist and converts the .nxml file to .md
python3 download_and_convert_to_md.py --filelist data/permissive_filelist.txt --total_docs "${TOTAL_DOCS}"
