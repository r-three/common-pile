#!/usr/bin/env sh

set -e

./get-indices.sh
./download-pages.sh
./parse-pages.sh
