#!/usr/bin/env sh

# for NEWS in democracy_no
python news/process.py --url https://www.propublica.org/ --output_dir data/news-propublica/ --keywords article
