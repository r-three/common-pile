#!/usr/bin/env sh

mkdir -p data/arxiv-abstracts/raw
cd data/arxiv-abstracts/raw

kaggle datasets download Cornell-University/arxiv
unzip arxiv.zip
