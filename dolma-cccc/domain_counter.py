from huggingface_hub import snapshot_download

import os
import gzip
import json
from urllib.parse import urlparse
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed, wait, FIRST_EXCEPTION
from tqdm import tqdm
import gc
import signal
import sys
from datasets import Dataset, load_dataset


def process_file(file_path):
    suburl_counter = defaultdict(int)
    text_length_counter = defaultdict(int)  # Additional counter based on text length

    # First, count the total number of lines in the file for progress bar initialization
    # with gzip.open(file_path, 'rt', encoding='utf-8') as f:
    #     total_lines = sum(1 for _ in f)

    # Re-open the file and process with progress bar
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        with tqdm(desc=f"Processing {os.path.basename(file_path)}", leave=False,
                  position=1) as line_progress:
            for line in f:
                try:
                    data = json.loads(line)
                    url = data.get('metadata', {}).get('warc_url', '')
                    text = data.get('text', '')
                    text_length = len(text)

                    if url:
                        suburls = extract_suburls(url)
                        for suburl in suburls:
                            suburl_counter[suburl] += 1
                            text_length_counter[suburl] += text_length  # Increment based on text length

                    line_progress.update(1)  # Update progress bar for each processed line
                except json.JSONDecodeError:
                    continue

    gc.collect()  # Explicitly trigger garbage collection to free memory
    return suburl_counter, text_length_counter


def extract_suburls(url):
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    path_parts = parsed_url.path.strip('/').split('/')

    suburls = [domain, ]
    current_path = domain

    for part in path_parts[:-1]:
        current_path = f"{current_path}/{part}"
        suburls.append(current_path)

    return suburls


def merge_counters(counter_list):
    merged_counter = defaultdict(int)
    for counter in counter_list:
        for suburl, count in counter.items():
            merged_counter[suburl] += count
    return merged_counter


def process_all_files_in_parallel(root_folder):
    all_files = []

    for dirpath, _, filenames in os.walk(root_folder):
        for filename in filenames:
            if filename.endswith('.json.gz'):
                all_files.append(os.path.join(dirpath, filename))

    def signal_handler(sig, frame):
        print("Interrupt received, terminating...")
        executor.shutdown(wait=False)
        sys.exit(1)

    # Register the signal handler
    signal.signal(signal.SIGINT, signal_handler)

    with tqdm(total=len(all_files), desc="Total files", position=0) as file_progress:
        with ProcessPoolExecutor(max_workers=40) as executor:  # Adjust max_workers based on your system
            future_to_file = {executor.submit(process_file, file): file for file in all_files}

            suburl_results = []
            text_length_results = []

            try:
                for future in tqdm(as_completed(future_to_file), total=len(future_to_file), desc="Processing files"):
                    try:
                        suburl_counter, text_length_counter = future.result()
                        suburl_results.append(suburl_counter)
                        text_length_results.append(text_length_counter)
                    except Exception as e:
                        print(f"Error processing file {future_to_file[future]}: {e}")
                    finally:
                        file_progress.update(1)  # Update the outer file-level progress bar

                wait(future_to_file.keys(), return_when=FIRST_EXCEPTION)

            except KeyboardInterrupt:
                print("Ctrl+C pressed, terminating...")
                executor.shutdown(wait=False)
                sys.exit(1)

    final_suburl_counter = merge_counters(suburl_results)
    final_text_length_counter = merge_counters(text_length_results)

    sorted_suburls = sorted(final_suburl_counter.items(), key=lambda item: item[1], reverse=True)
    print("Top 10 SubURLs by frequency:")
    for suburl, count in sorted_suburls[:10]:
        print(f"{suburl}: {count}")

    sorted_text_lengths = sorted(final_text_length_counter.items(), key=lambda item: item[1], reverse=True)
    print("\nTop 10 SubURLs by total text length:")
    for suburl, total_length in sorted_text_lengths[:10]:
        print(f"{suburl}: {total_length}")

    return final_suburl_counter, final_text_length_counter


def calculate_level(suburl):
    return suburl.count('/')  # Number of slashes indicates the level


def find_parent_url(url):
    """
    Returns the parent URL by removing the last segment.
    For example, 'AA/BB/CC' -> 'AA/BB'
    """
    parts = url.rstrip('/').split('/')
    if len(parts) > 1:
        return '/'.join(parts[:-1])
    return None

def remove_redundant_urls(dataset):
    """
    Remove redundant URLs from the dataset if its parent urls has the same sample count and text length
    Args:
        dataset:

    Returns:

    """
    # Create a dictionary to map each URL to its data
    url_dict = {row['suburl']: row for row in dataset}

    # Track URLs to remove
    urls_to_remove = set()

    # Iterate over each URL and compare it with its parent URL
    for url, data in tqdm(url_dict.items()):
        parent_url = find_parent_url(url)
        if parent_url and parent_url in url_dict:
            parent_data = url_dict[parent_url]
            if (data['total_sample_count'] == parent_data['total_sample_count'] and
                    data['total_text_length'] == parent_data['total_text_length']):
                # If counts match, mark the higher-level URL for removal
                urls_to_remove.add(url)

    print(f"{len(urls_to_remove)}/{len(url_dict)} URL to be removed")

    # Filter out the redundant URLs
    filtered_data = [row for row in dataset if row['suburl'] not in urls_to_remove]

    return filtered_data

# download the dolma-ccc dataset for analysis
snapshot_download(repo_id="allenai/dolma-cccc", local_dir='data/dolma-cccc', repo_type="dataset")

# Define the dataset name (this should be unique within your Hugging Face namespace)
dataset_name = "wildphoton/dolma-cccc-suburl-stats"
# Run the processing
root_folder = './data/dolma-cccc/data'  # Update with your root folder path

# # Define the dataset name (this should be unique within your Hugging Face namespace)
# dataset_name = "wildphoton/dolma-cccc-suburl-stats-CC-MAIN-2024-18"
# # Run the processing
# root_folder = './data/dolma-cccc/data/CC-MAIN-2024-18'  # Update with your root folder path

token = "YOUR_HF_TOKEN"  # Replace with you own hf token if you want to submit the dataset


# Counting urls and text length
final_suburl_counter, final_text_length_counter = process_all_files_in_parallel(root_folder)

# Convert the counters to lists of dictionaries
suburl_data = [{"suburl": suburl, "total_sample_count": count} for suburl, count in final_suburl_counter.items()]
text_length_data = [{"suburl": suburl, "total_text_length": total_length} for suburl, total_length in final_text_length_counter.items()]

# Convert the lists to dictionaries indexed by "suburl"
suburl_dict = {item["suburl"]: item["total_sample_count"] for item in suburl_data}
text_length_dict = {item["suburl"]: item["total_text_length"] for item in text_length_data}

# Merge the dictionaries
merged_data = []
for suburl in suburl_dict:
    merged_data.append({
        "suburl": suburl,
        "total_sample_count": suburl_dict[suburl],
        "total_text_length": text_length_dict.get(suburl, 0),
        "url_level": calculate_level(suburl)
    })

# Create a Hugging Face dataset from the merged data
merged_dataset = Dataset.from_list(merged_data)

# Inspect the first few rows
print(merged_dataset)

# Push the dataset to the Hugging Face Hub
# By here, we create the dataset at https://huggingface.co/datasets/wildphoton/dolma-cccc-suburl-stats
merged_dataset.push_to_hub(dataset_name, token=token)


# load the dataset

cache_dir = f"./data/{dataset_name.split('/')[-1]}"

snapshot_download(repo_id=dataset_name, local_dir=cache_dir, repo_type="dataset")

dataset = load_dataset("parquet", cache_dir=cache_dir, data_dir=cache_dir, split="train")
print(dataset[:10])

# Apply the post-processing function
filtered_data = remove_redundant_urls(dataset)

# Convert the filtered data back to a Hugging Face Dataset
filtered_dataset = Dataset.from_list(filtered_data)

# # # Save or push the filtered dataset
filtered_dataset.push_to_hub(dataset_name+"_merged", token=token)