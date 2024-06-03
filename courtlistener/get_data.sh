#!/bin/bash
set -e

# URL of the directory
base_url="https://storage.courtlistener.com/bulk-data/"

# Define the download directory
download_dir="./data/courtlistener/raw"

# Create the directory if it does not exist
mkdir -p "$download_dir"

dates=(
    "2024-05-06"
)

max_jobs=8

# Parse command-line options
while [[ $# -gt 0 ]]; do
    key="$1"
    case $key in
        --test_run)
            # Use the first N dates for testing
            shift
            test_run_count=$1
            dates=("${dates[@]:0:$test_run_count}")
            shift
            ;;
        --max_jobs)
            # Set the maximum number of parallel jobs
            shift
            max_jobs=$1
            shift
            ;;
        *)
            echo "Unknown option: $key"
            exit 1
            ;;
    esac
done

# Display the dates of the files to be fetched
echo "Dates of files to be fetched:"
for date in "${dates[@]}"; do
    echo "$date"
done

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
