#!/usr/bin/env sh

set -e

./get-chats.sh
python to-dolma.py
python preprocess.py
