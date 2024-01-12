#!/usr/bin/env sh

# CC-BY
python news/get_page.py --url https://360info.org/ --output_dir data/news/raw/360info/ --filename news-360info.jsonl.gz
python news/get_page.py --url https://meduza.io/ --output_dir data/news/raw/meduza/ --filename news-meduza.jsonl.gz
python news/get_page.py --url https://www.altnews.in/ --output_dir data/news/raw/altnews/ --filename news-altnews.jsonl.gz
python news/get_page.py --url http://scidev.net/ --output_dir data/news/raw/scidev/ --filename news-scidev.jsonl.gz
python news/get_page.py --url https://www.fides.org/en --output_dir data/news/raw/fides/ --filename news-fides.jsonl.gz
python news/get_page.py --url https://factly.in/ --output_dir data/news/raw/factly/ --filename news-factly.jsonl.gz
python news/get_page.py --url https://milwaukeenns.org/ --output_dir data/news/raw/milwaukeenns/ --filename news-milwaukeenns.jsonl.gz
python news/get_page.py --url https://www.tasnimnews.com/en --output_dir data/news/raw/tasnimnews/ --filename news-tasnimnews.jsonl.gz
python news/get_page.py --url https://www.mekongeye.com/ --output_dir data/news/raw/mekongeye/ --filename news-mekongeye.jsonl.gz
python news/get_page.py --url https://africasacountry.com/ --output_dir data/news/raw/africasacountry/ --filename news-africasacountry.jsonl.gz
python news/get_page.py --url https://balkandiskurs.com/en/ --output_dir data/news/raw/balkandiskurs/ --filename news-balkandiskurs.jsonl.gz
python news/get_page.py --url https://minorityafrica.org/ --output_dir data/news/raw/minorityafrica/ --filename news-minorityafrica.jsonl.gz
python news/get_page.py --url https://zimfact.org/ --output_dir data/news/raw/zimfact/ --filename news-zimfact.jsonl.gz
python news/get_page.py --url https://globalvoices.org/ --output_dir data/news/raw/globalvoices/ --filename news-globalvoices.jsonl.gz
python news/get_page.py --url https://sojoexchange.solutionsjournalism.org/ --output_dir data/news/raw/solutionsjournalism/ --filename news-solutionsjournalism.jsonl.gz
python news/get_page.py --url https://freedom.press/ --output_dir data/news/raw/freedom/ --filename news-freedom.jsonl.gz
python news/get_page.py --url https://www.newcanadianmedia.ca/ --output_dir data/news/raw/newcanadianmedia/ --filename news-newcanadianmedia.jsonl.gz
python news/get_page.py --url https://projectmultatuli.org/en/ --output_dir data/news/raw/projectmultatuli/ --filename news-projectmultatuli.jsonl.gz

# CC BY-SA
python news/get_page.py --url https://www.propastop.org/eng/ --output_dir data/news/raw/propastop/ --filename news-propastop.jsonl.gz
python news/get_page.py --url https://www.thepublicrecord.ca/ --output_dir data/news/raw/thepublicrecord/ --filename news-thepublicrecord.jsonl.gz
python news/get_page.py --url https://educeleb.com/ --output_dir data/news/raw/educeleb/ --filename news-educeleb.jsonl.gz
python news/get_page.py --url https://oxpeckers.org/ --output_dir data/news/raw/oxpeckers/ --filename news-oxpeckers.jsonl.gz
python news/get_page.py --url https://libertytvradio.com/ --output_dir data/news/raw/libertytvradio/ --filename news-libertytvradio.jsonl.gz

# Public Domain
python news/get_page.py --url https://central.asia-news.com/en_GB/ --output_dir data/news/raw/caravanserai/ --filename news-caravanserai.jsonl.gz