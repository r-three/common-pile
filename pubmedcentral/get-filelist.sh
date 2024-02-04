#!/usr/bash/env sh

NONPERMISSIVE_LICENSES="NO-CC CODE\|CC BY-NC\|CC BY-ND\|CC BY-NC-ND\|CC BY-NC-NA\|CC BY-NC-SA"

# download full filelist
# -nc: no clobber, do not overwrite existing files
wget -nc https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_file_list.txt -P data/

PERMISSIVE_FILELIST_DEST="data/permissive_filelist.txt"
# -v invert match (find lines without these patterns), -w match whole word
grep -vw "${NONPERMISSIVE_LICENSES}" data/oa_file_list.txt > ${PERMISSIVE_FILELIST_DEST}

# don't count the header line as a file
echo "Found $(($(wc -l < ${PERMISSIVE_FILELIST_DEST})-1)) permissive licenses in data/oa_file_list.txt"
