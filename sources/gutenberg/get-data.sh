#!/usr/bin/env sh

set -e

./get-metadata.sh
python build-index.py
python add-to-book-index.py
python get-books.py
python get-pg19-books.py
python to-dolma.py
python preprocess.py
