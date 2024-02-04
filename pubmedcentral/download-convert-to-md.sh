#!/usr/bash/env sh

# downloads each file from the filelist and converts the .nxml file to .md
python3 download_and_convert_to_md.py --filelist data/permissive_filelist.txt
