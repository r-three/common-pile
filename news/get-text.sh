#!/usr/bin/env sh

# CC-BY
python news/get_text.py --input_dir data/news/raw/360info/ --filename news-360info.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "copy main-copy"}'
python news/get_text.py --input_dir data/news/raw/meduza/ --filename news-meduza.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/altnews/ --filename news-altnews.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/scidev/ --filename news-scidev.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/fides/ --filename news-fides.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/factly/ --filename news-factly.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/milwaukeenns/ --filename news-milwaukeenns.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/tasnimnews/ --filename news-tasnimnews.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/mekongeye/ --filename news-mekongeye.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/africasacountry/ --filename news-africasacountry.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/balkandiskurs/ --filename news-balkandiskurs.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/minorityafrica/ --filename news-minorityafrica.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/zimfact/ --filename news-zimfact.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/globalvoices/ --filename news-globalvoices.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/solutionsjournalism/ --filename news-solutionsjournalism.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/freedom/ --filename news-freedom.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/newcanadianmedia/ --filename news-newcanadianmedia.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/projectmultatuli/ --filename news-projectmultatuli.jsonl.gz --output_dir data/news/

# CC BY-SA
python news/get_text.py --input_dir data/news/raw/propastop/ --filename news-propastop.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/thepublicrecord/ --filename news-thepublicrecord.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/educeleb/ --filename news-educeleb.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/oxpeckers/ --filename news-oxpeckers.jsonl.gz --output_dir data/news/
python news/get_text.py --input_dir data/news/raw/libertytvradio/ --filename news-libertytvradio.jsonl.gz --output_dir data/news/

# Public Domain
python news/get_text.py --input_dir data/news/raw/caravanserai/ --filename news-caravanserai.jsonl.gz --output_dir data/news/
