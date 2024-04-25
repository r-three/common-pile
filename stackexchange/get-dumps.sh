#!/usr/bin/env sh

# Get all the stackexchange dumps from archive.org
wget https://archive.org/download/stackexchange/Sites.xml -P data/
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
python list-sites.py | shuf | xargs -n10 -P4 wget -w 2 -t 10 -c -nc -P data/dump --show-progress
./extract.sh
./get-stackoverflow.sh
