#!/usr/bin/env sh

for file in data/dump/*.7z; do
    7z x -o${file%.7z} "${file}"
done
