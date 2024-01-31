#!/usr/bin/env sh

NONPERMISSIVE_LICENSES="CC BY-NC\|CC BY-ND\|CC BY-NC-ND\|CC BY-NC-NA"
BASE_URL="https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/xml"

# get all the pubmed-central dumps
for i in {00..10}; do

    # Vars for text files
    # BASE_URL="https://ftp.ncbi.nlm.nih.gov/pub/pmc/oa_bulk/oa_comm/txt"
    # FILELIST_URL="${BASE_URL}/oa_comm_txt.PMC0${i}xxxxxx.baseline.2023-12-17.filelist.txt"
    # FILES_URL="${BASE_URL}/oa_comm_txt.PMC0${i}xxxxxx.baseline.2023-12-17.tar.gz"
    # FILES_DEST="data/oa_comm_txt.PMC0${i}xxxxxx.baseline.2023-12-17.tar.gz"

    # Vars for xml files
    FILELIST_URL="${BASE_URL}/oa_comm_xml.PMC0${i}xxxxxx.baseline.2023-12-18.filelist.txt"
    FILES_URL="${BASE_URL}/oa_comm_xml.PMC0${i}xxxxxx.baseline.2023-12-18.tar.gz"
    FILES_DEST="data/oa_comm_xml.PMC0${i}xxxxxx.baseline.2023-12-18.tar.gz"

    # download files
    # -nc: no clobber, do not overwrite existing files
    wget -nc ${FILELIST_URL} -P data/
    wget -nc ${FILES_URL} -P data/

    # extract files
    tar -xzf ${FILES_DEST} -C data/

    # remove tar files
    rm ${FILES_DEST}

    # filter filelist for permissive licenses
    FILELIST_DEST="data/oa_comm_xml.PMC0${i}xxxxxx.baseline.2023-12-18.filelist.txt"
    PERMISSIVE_FILELIST_DEST="data/oa_comm_xml.PMC0${i}xxxxxx.baseline.2023-12-18.permissive_filelist.txt"
    # -v invert match (find lines without these patterns), -w match whole word
    grep -vw "${NONPERMISSIVE_LICENSES}" ${FILELIST_DEST} > ${PERMISSIVE_FILELIST_DEST}

    echo "Found $(($(wc -l < ${PERMISSIVE_FILELIST_DEST})-1)) permissive licenses in ${FILELIST_DEST}"

    # remove filelist
    rm ${FILELIST_DEST}

done
