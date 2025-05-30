#!/usr/bin/env bash

data_dir=${1:-"data"}
data_dir=${data_dir%/}

for site_dump in ${data_dir}/stackexchange/v0/*/; do
  site=$(basename ${site_dump})
  if [[ "${site}" != "stackoverflow.com" ]]; then
    ids="${data_dir}/stackexchange/v0/${site}/ids.json"
    old="${data_dir}/stackexchange-old/v0/${site}"
    echo "python merge-dolma.py --input ${site_dump} --ids ${ids} --old ${old}"
    time python merge-dolma.py --input "${site_dump}" --ids "${ids}" --old "${old}"
  fi
done
