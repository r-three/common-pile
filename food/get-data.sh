#!/usr/bin/env sh

set -e

DATA_DIR=${1:-"data"}
DATA_DIR=${DATA_DIR%/}
TEST_RUN=${2:-0}
WAIT=${3:-2}

python build_index.py --index_path ${DATA_DIR}/pages/page_index.jsonl
python download_pages.py --index_path ${DATA_DIR}/pages/page_index.jsonl --test_run "${TEST_RUN}" --wait "${WAIT}"
python to_dolma.py --index_path ${DATA_DIR}/pages/page_index.jsonl --output_dir ${DATA_DIR}/foodista/raw/documents
python preprocess.py
