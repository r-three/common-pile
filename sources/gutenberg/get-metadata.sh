#! /usr/bin/env bash

set -e

METADATA="https://gutenberg.org/cache/epub/feeds/rdf-files.tar.bz2"
WGET_TARGET="rdf-files.tar.bz2"

mkdir -p data

if [[ ! -f "data/${WGET_TARGET}" ]]; then
  echo "Downloading RDF metadata files."
  wget "${METADATA}" -O "data/${WGET_TARGET}"
else
  echo "Metadata already downloaded."
fi

if [[ ! -d "data/cache" ]]; then
  echo "Unpacking RDF metadata files."
  tar -xf "data/${WGET_TARGET}" -C "data/"
else
  echo "Metadata already unpacked."
fi
