#!/usr/bin/env sh

set -e

TEST_RUN=${1:-0}
WAIT=${2:-2}

python build_index.py
python download_pages.py --test_run "${TEST_RUN}" --wait "${WAIT}"
python to_dolma.py
python preprocess.py
