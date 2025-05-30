#!/usr/bin/env bash

data_dir=${1:-"data"}
data_dir=${data_dir%/}
test_run=${2:-"0"}

declare -a sites=(
    360info
    # africasacountry
    altnews
    balkandiskurs
    factly
    # fides
    freedom
    globalvoices
    # meduza
    mekongeye
    milwaukeenns
    minorityafrica
    newcanadianmedia
    # scidev
    solutionsjournalism
    # tasnimnews
    zimfact
    educeleb
    libertytvradio
    oxpeckers
    propastop
    thepublicrecord
    # caravanserai
)

for site in ${sites[@]}; do
    python download_pages.py --index_path ${data_dir}/pages/${site}/pagelist.jsonl --test_run ${test_run}
done
