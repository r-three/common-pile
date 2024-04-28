#! /usr/bin/env bash

set -e

PDR_DIRECTORY="data/public-domain-review"

mkdir -p ${PDR_DIRECTORY}

echo "Scraping Collections"
python public_domain_review/scrape-collections.py 

echo "Scraping Conjectures"
python public_domain_review/scrape-conjectures.py 

echo "Scraping Essays"
python public_domain_review/scrape-essays.py 
