set -e

REGULATIONS_DIRECTORY="data/regulations"


echo "Copying bulk download files from GCS"
mkdir -p ${REGULATIONS_DIRECTORY}/raw
gsutil -m cp -r gs://regulations_data/* ${REGULATIONS_DIRECTORY}/raw

echo "Building Document Indexes"
python build-index.py --input-dir ${REGULATIONS_DIRECTORY}/raw --output-dir ${REGULATIONS_DIRECTORY}/indexes

echo "Downloading Files"
python download-files.py --input-dir ${REGULATIONS_DIRECTORY}/indexes --output-dir ${REGULATIONS_DIRECTORY}/files

echo "Converting to Text Files"
python convert.py --input-dir ${REGULATIONS_DIRECTORY}/files --output-dir ${REGULATIONS_DIRECTORY}/text_files

echo "Creating Dolma Dataset"
python to-dolma.py --file-dir ${REGULATIONS_DIRECTORY}/text_files --index-dir ${REGULATIONS_DIRECTORY}/indexes --output-dir ${REGULATIONS_DIRECTORY}/v0
