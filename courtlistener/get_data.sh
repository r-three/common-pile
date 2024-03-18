#!/bin/bash
set -e

# URL of the directory
base_url="https://storage.courtlistener.com/bulk-data/"

# Define the download directory
download_dir="./data/courtlistener/raw"

# Create the directory if it does not exist
mkdir -p "$download_dir"

dates=(
    "2022-08-02"
#    "2022-08-31"
#    "2022-09-30"
#    "2022-10-31"
#    "2022-11-30"
#    "2022-12-31"
#    "2023-01-31"
#    "2023-02-28"
#    "2023-03-31"
#    "2023-04-30"
#    "2023-05-31"
#    "2023-07-31"
#    "2023-08-31"
#    "2023-12-04"
)

max_jobs=4

# Function to download and decompress a file
download_and_decompress() {
    local file_name="opinions-${1}.csv"
#    local file_name="financial-disclosure-investments-${1}.csv"
    local file_url="${base_url}${file_name}.bz2"
    local decompressed_file="${download_dir}/${file_name}"
    local compressed_file="${download_dir}/${file_name}.bz2"

    # Check if the decompressed file already exists
    if [[ -f "$decompressed_file" ]]; then
        echo "Decompressed file ${decompressed_file} already exists, skipping..."
    else
      # Check if the compressed file already exists
      if [[ -f "$compressed_file" ]]; then
          echo "Compressed file ${compressed_file} already exists, skipping download..."
      else
          # Download the file
          wget -P "$download_dir" "$file_url"
      fi
      # Decompress the file
      bunzip2 "$compressed_file"
      echo "Decompressed file ${compressed_file} ..."
    fi

    # transform csv files into shared dolma data
    echo "Save records in ${decompressed_file} to dolma data"
    python ./courtlistener/csv_to_dolma.py --input_file ${decompressed_file}
}


# Download each file
for date in "${dates[@]}"; do
    download_and_decompress "$date" &

    # Limit the number of parallel jobs
    if (( $(jobs -r | wc -l) >= max_jobs )); then
        wait -n
    fi
done

# Wait for all background jobs to finish
wait

echo "Download and decompression completed."