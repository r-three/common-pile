#!/usr/bin/env sh

# Recursivly get files from https://irclogs.ubuntu.com
# -np don't look into the parent of the root
# -nc don't overwrite files that are already there
# -l 4 only go 4 levels deep
# -A txt only get txt files
# -P data/ Save the output in ./data/
wget -r -np -nc -l 4 -A txt -P data/ https://irclogs.ubuntu.com
