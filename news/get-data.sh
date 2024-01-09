#!/usr/bin/env sh

python news/get_page.py --url https://www.propublica.org/ --output_dir data/news-propublica/
python news/get_page.py --url https://www.democracynow.org/ --output_dir data/news-democracy_now/ 
python news/get_page.py --url https://www.mongabay.com/ --output_dir data/news-mongabay/
python news/get_page.py --url https://theconversation.com/ --output_dir data/news-theconversation/ 

