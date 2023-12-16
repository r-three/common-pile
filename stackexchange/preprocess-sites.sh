#!/usr/bin/env bash

for site_dump in data/dump/*/; do
  site=$(basename ${site_dump})
  if [[ "${site}" != "stackoverflow.com" ]]; then
    output="data/stack-exchange/v0/${site}"
    if [[ ! -d ${output} ]]; then
      echo "python preprocess.py --input ${site_dump} --output data/stack-exchange/v0/${site}"
      time python preprocess.py --input ${site_dump} --output data/stack-exchange/v0/${site}
    fi
  fi
done
