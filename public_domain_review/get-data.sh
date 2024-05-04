#! /usr/bin/env bash

set -e
# Default to a test run of zero, meaning getting all the data.
test_run=${1:0}

PDR_DIRECTORY="data/public-domain-review"

mkdir -p ${PDR_DIRECTORY}

echo "Scraping Collections"
python scrape.py --type collection --test_run "${test_run}"

echo "Scraping Conjectures"
python scrape.py --type conjecture --test_run "${test_run}"

echo "Scraping Essays"
python scrape.py --type essay --test_run "${test_run}"
