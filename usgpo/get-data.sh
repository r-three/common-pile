set -e

api_key=${1}
start_date=${2:-"1990-01-01'T'00:00:00'Z'"}

USGPO_DIRECTORY="data/usgpo"

mkdir -p ${USGPO_DIRECTORY}/raw

echo "Getting Document Links"
python usgpo/get-links.py --api-key "${api_key}" --start-date "${start_date}" --output-dir ${USGPO_DIRECTORY}/raw

echo "Downloading Documents"
python usgpo/download-files.py --api-key ${api_key} --links-file ${USGPO_DIRECTORY}/raw/links.jsonl
