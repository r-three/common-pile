#!/usr/bin/env sh

# CC BY
python news/get_text.py --license CC BY --input_dir data/news/raw/360info/ --filename news-360info.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "copy main-copy"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/africasacountry/ --filename news-africasacountry.jsonl.gz --output_dir data/news/ --tag article --attrs '{"class": "po__article"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/altnews/ --filename news-altnews.jsonl.gz --output_dir data/news/ --tag div --attrs '{"id": "primary"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/balkandiskurs/ --filename news-balkandiskurs.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "entry-content"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/factly/ --filename news-factly.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "post-content-right"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/fides/ --filename news-fides.jsonl.gz --output_dir data/news/ --tag div --attrs '{"id": "news"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/freedom/ --filename news-freedom.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "blog-page"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/globalvoices/ --filename news-globalvoices.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "entry-container"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/meduza/ --filename news-meduza.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "article"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/mekongeye/ --filename news-mekongeye.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "main-content"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/milwaukeenns/ --filename news-milwaukeenns.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "entry-content"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/minorityafrica/ --filename news-minorityafrica.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "post-content-container"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/newcanadianmedia/ --filename news-newcanadianmedia.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "content-main"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/scidev/ --filename news-scidev.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "fl-col-content fl-node-content"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/solutionsjournalism/ --filename news-solutionsjournalism.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "sqs-html-content"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/tasnimnews/ --filename news-tasnimnews.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "story"}'
python news/get_text.py --license CC BY --input_dir data/news/raw/zimfact/ --filename news-zimfact.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "entry-content"}'

# CC BY-SA
python news/get_text.py --license CC BY-SA --input_dir data/news/raw/educeleb/ --filename news-educeleb.jsonl.gz --output_dir data/news/
python news/get_text.py --license CC BY-SA --input_dir data/news/raw/libertytvradio/ --filename news-libertytvradio.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "td-post-content"}'
python news/get_text.py --license CC BY-SA --input_dir data/news/raw/oxpeckers/ --filename news-oxpeckers.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "post_text_inner"}' 
python news/get_text.py --license CC BY-SA --input_dir data/news/raw/propastop/ --filename news-propastop.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "post-wrap"}'
python news/get_text.py --license CC BY-SA --input_dir data/news/raw/thepublicrecord/ --filename news-thepublicrecord.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "entry-content"}'

# Public Domain
python news/get_text.py --license Public Domain --input_dir data/news/raw/caravanserai/ --filename news-caravanserai.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "article__content"}'

# CC NC ND
# python news/get_text.py --license CC NC ND --input_dir data/news/raw/projectmultatuli/ --filename news-projectmultatuli.jsonl.gz --output_dir data/news/ --tag div --attrs '{"class": "elementor-widget-container"}'