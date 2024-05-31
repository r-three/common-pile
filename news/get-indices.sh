#!/usr/bin/env sh

data_dir=${1:-"data"}
data_dir=${data_dir%/}

# CC-BY
python build_index.py --url https://360info.org/ --index_path ${data_dir}/pages/360info/pagelist.jsonl
# python build_index.py --url https://africasacountry.com/ --index_path ${data_dir}/pages/africasacountry/pagelist.jsonl
python build_index.py --url https://www.altnews.in/ --index_path ${data_dir}/pages/altnews/pagelist.jsonl
python build_index.py --url https://balkandiskurs.com/en/ --index_path ${data_dir}/pages/balkandiskurs/pagelist.jsonl
python build_index.py --url https://factly.in/ --index_path ${data_dir}/pages/factly/pagelist.jsonl
# python build_index.py --url https://www.fides.org/en --index_path ${data_dir}/pages/fides/pagelist.jsonl
python build_index.py --url https://freedom.press/ --index_path ${data_dir}/pages/freedom/pagelist.jsonl
python build_index.py --url https://globalvoices.org/ --index_path ${data_dir}/pages/globalvoices/pagelist.jsonl
# python build_index.py --url https://meduza.io/en --index_path ${data_dir}/pages/meduza/pagelist.jsonl
python build_index.py --url https://www.mekongeye.com/ --index_path ${data_dir}/pages/mekongeye/pagelist.jsonl
python build_index.py --url https://milwaukeenns.org/ --index_path ${data_dir}/pages/milwaukeenns/pagelist.jsonl
python build_index.py --url https://minorityafrica.org/ --index_path ${data_dir}/pages/minorityafrica/pagelist.jsonl
python build_index.py --url https://www.newcanadianmedia.ca/ --index_path ${data_dir}/pages/newcanadianmedia/pagelist.jsonl
# python build_index.py --url http://scidev.net/ --index_path ${data_dir}/pages/scidev/pagelist.jsonl
python build_index.py --url https://sojoexchange.solutionsjournalism.org/ --index_path ${data_dir}/pages/solutionsjournalism/pagelist.jsonl
# python build_index.py --url https://www.tasnimnews.com/en --index_path ${data_dir}/pages/tasnimnews/pagelist.jsonl
python build_index.py --url https://zimfact.org/ --index_path ${data_dir}/pages/zimfact/pagelist.jsonl

# CC BY-SA
python build_index.py --url https://educeleb.com/ --index_path ${data_dir}/pages/educeleb/pagelist.jsonl
python build_index.py --url https://libertytvradio.com/ --index_path ${data_dir}/pages/libertytvradio/pagelist.jsonl
python build_index.py --url https://oxpeckers.org/ --index_path ${data_dir}/pages/oxpeckers/pagelist.jsonl
python build_index.py --url https://www.propastop.org/eng/ --index_path ${data_dir}/pages/propastop/pagelist.jsonl
python build_index.py --url https://www.thepublicrecord.ca/ --index_path ${data_dir}/pages/thepublicrecord/pagelist.jsonl

# Public Domain
# python build_index.py --url https://central.asia-news.com/en_GB/ --index_path ${data_dir}/pages/caravanserai/pagelist.jsonl
