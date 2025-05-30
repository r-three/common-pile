#! /usr/bin/env bash

set -e

METADATA_URL="https://www.biodiversitylibrary.org/data/bhltitle.mods.xml.zip"
METADATA_FILE="bhltitle.mods.xml.zip"
UNZIPPED_METADATA_FILE="bhltitle.mods.xml"
BHL_DIRECTORY="data/biodiversity-heritage-library/raw"

mkdir -p ${BHL_DIRECTORY}

if [[ ! -f "${BHL_DIRECTORY}/${METADATA_FILE}" ]]; then
  echo "Downloading metadata file."
  curl -L -o "${BHL_DIRECTORY}/${METADATA_FILE}" ${METADATA_URL}
else
  echo "Metadata already downloaded."
fi

echo "Unpacking zipped metadata files."
unzip "${BHL_DIRECTORY}/${METADATA_FILE}" -d ${BHL_DIRECTORY}
mv "${BHL_DIRECTORY}/data" "${BHL_DIRECTORY}/metadata"
