#! /usr/bin/env bash

set -e

DATA_URL="https://smithsonian.figshare.com/ndownloader/files/43189896"
DATA_FILE="bhl-ocr-20230823.tar.bz2"
BHL_DIRECTORY="data/biodiversity-heritage-library/raw/data"

mkdir -p ${BHL_DIRECTORY}

if [[ ! -f "${BHL_DIRECTORY}/${DATA_FILE}" ]]; then
  echo "Downloading data file."
  curl -L -o "${BHL_DIRECTORY}/${DATA_FILE}" ${DATA_URL}
else
  echo "Metadata already downloaded."
fi
