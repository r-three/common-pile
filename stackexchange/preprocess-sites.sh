#!/usr/bin/env bash

data_dir=${1:-"data"}
data_dir=${data_dir%/}

for site_dump in ${data_dir}/dump/*/; do
  site=$(basename ${site_dump})
  if [[ "${site}" != "stackoverflow.com" ]]; then
    output="${data_dir}/stackexchange/v0/${site}"
    if [[ ! -d ${output} ]]; then
      echo "python preprocess.py --input ${site_dump} --output ${output}"
      time python preprocess.py --input ${site_dump} --output ${output}
    fi
  fi
done
