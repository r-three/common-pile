#!/usr/bin/env bash

data_dir=${1:-"data"}
data_dir=${data_dir%/}

site_url=${2:-"https://archive.org/download/stackexchange/Sites.xml"}
site_file=`python -c "import os, urllib; print(os.path.basename(urllib.parse.urlparse('${site_url}').path))"`
if [[ -z "${site_file}" ]]; then
    site_file="index.html"
fi
site_path="${data_dir}/${site_file}"
if [[ "${site_path}" == *.xml ]]; then
    format="xml"
    base_url=`python -c "import os; print(os.path.dirname('${site_url}'))"`
else
    format="index"
    base_url="${site_url}"
fi

echo "Downloading Sites map from ${site_url} and saving to ${data_dir}."
# Get all the stackexchange dumps from archive.org
wget "${site_url}" -P "${data_dir}"
# List the download links based on the xml file and the archive.org base dir.
# Shuffle so any restarts probably start with a new files
# run 4 wget processins and call each on with 10 urls.
# -w 2           wait 2 seconds between downloads
# -t 10          retry a url 10 times
# -c             continue partial downloads
# -nc            don't redownload something that is already there
# -P data/dump   save all files with the prefix data/dump
# Note: The progress bars from the different wget processes overwrite each other
# They are still useful to ensure that progress is being made.
python list-sites.py --sites "${site_path}" --format "${format}" --base_url "${base_url}" | shuf | xargs -n10 -P4 wget -w 2 -t 10 -c -nc -P ${data_dir}/dump --show-progress

# Community dumps don't include the windowsphone site (as it is defunct). Download them
# from the most recent offical dump (updated 2025/03/11)
if [[ "${format}" == "index" ]]; then
    echo "Downloading last office windowsphone dump as you are using a community dump"
    wget "https://archive.org/download/stackexchange/windowsphone.stackexchange.com.7z" -P "${data_dir}"
    wget "https://archive.org/download/stackexchange/windowsphone.meta.stackexchange.com.7z" -P "${data_dir}"
fi

./extract.sh ${data_dir}
# Official dumps have stack overflow (the largest site) split into multiple files
# This script downloads each one. Community dumps have a single .7z for it.
if [[ "${format}" == "xml" ]]; then
    echo "Downloading each part of stackoverflow was you are using an official dump."
    ./get-stackoverflow.sh ${data_dir} ${base_url}
fi
