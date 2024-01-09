#!/usr/bin/env sh

python news/process.py --url https://www.propublica.org/ --filename pro.jsonl.gz --output_dir data/news-propublica/ --keywords article
python news/process.py --url https://www.democracynow.org/ --filename dem.jsonl.gz --output_dir data/news-democracy_now/ 

