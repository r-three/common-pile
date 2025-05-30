#!/usr/bin/env bash

data_dir=${1:-"data"}
data_dir=${data_dir%/}

for site_dump in ${data_dir}/stackexchange/v0/*/; do
  site=$(basename ${site_dump})
  if [[ "${site}" != "stackoverflow.com" ]]; then
    output="${data_dir}/stackexchange/v0/${site}/ids.json"
    if [[ ! -d ${output} ]]; then
      echo "python collect-ids.py --input ${site_dump} --output ${output}"
      time python collect-ids.py --input ${site_dump} --output ${output}
    fi
  fi
done

time python collect-ids.py --input "${data_dir}/stackexchange/v0/stackoverflow.com/" --output "${data_dir}/stackexchange/v0/stackoverflow.com/ids.json"
