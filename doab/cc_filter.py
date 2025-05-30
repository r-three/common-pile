import os
import shutil
import argparse

from tqdm import tqdm

CC_KEYWORDS = [
        "http://creativecommons.org/licenses/by/4.0", 
        "Creative Commons Attribution 3.0", 
        "Creative Commons Attribution 4.0", 
        "CC BY 4.0", 
        "CC BY-SA 4.0", 
        "Creative Commons Attribution CC-BY 4.0", 
        "Creative Commons By Attribution Share Alike", 
        "http://creativecommons.org/licenses/ by/3.0", 
        "http://creativecommons.org/licenses/by/3.0", 
        "https://creativecommons.org/licenses/by-sa/4.0", 
        "Creative Commons Attribution-ShareAlike 4.0", 
        "Creative Commons CC BY license", 
        "Creative Commons Attribution (CC-BY)", 
        "Creative Commons License CC-BY 4.0", 
        "available under a CC-BY license", 
        "https://creativecommons.org/licenses/by/4.0", 
        "Creative Commons Namensnennung - Weitergabe unter gleichen Bedingungen 4.0", 
        "Creative Commons Attribution – ShareAlike 4.0", 
        "https://creativecommons.org/licenses/by/3.0/pl", 
        "published under the most recent version of the Creative Commons CC-BY licence", 
        "distributed under the Creative Commons Attribution (CC BY) license", 
        "Individual articles may be downloaded and reproduced in accordance with the principles of the CC-BY licence", 
        "distributed under the Creative Commons Attribution License (CC BY)"
]

WHITELIST_IDS = [
        "015621cf-bf79-452d-9027-0e32a3c5f4df", 
        "03bf4fed-ca19-4975-9b4d-ff693a240606", 
        "03e2b665-8ef6-492e-b1d4-7d826b84548d", 
        "0453fb51-e028-4f32-95e2-c94632ce2f89", 
        "07206c08-8edf-4151-aab5-d2ad23020848", 
        "08837d35-6a67-4878-8a16-c7448467c859", 
        "0398d510-4a0d-4d99-b0db-88c368d0a89b", 
        "04d76735-c275-49d9-8e29-f1bab874e834", 
        "09724de8-643d-482d-a1f5-5206a05aa76e", 
        "0a80e530-ab8f-47bc-b5d5-4978089bebe8", 
        "0b94e427-6e72-4e5a-9c3b-78a928070053", 
        "0c5449f2-c36c-4900-9913-ba30acf7a185", 
        "10b167c4-8289-46d0-b8e9-80cf2f2c31a6", 
        "10fab9c8-8d12-414a-bad8-86106a7d297a", 
        "00134e2c-4cd5-4041-bb42-ea5f0ee762e6"
]


parser = argparse.ArgumentParser(description='Filter books based on explicit permissive license statements')
parser.add_argument('--input-files', nargs="+", help='Input files')
parser.add_argument('--output-dir', type=str, help='Path to the output directory')
args = parser.parse_args()


os.makedirs(args.output_dir, exist_ok=True)

copied_files = 0
pbar = tqdm(args.input_files)
for file in pbar:
    basename = os.path.basename(file)
    id = os.path.splitext(basename)[0]

    output_path = os.path.join(args.output_dir, id[:2], basename)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if id in WHITELIST_IDS:
        shutil.copy(file, output_path)
        copied_files += 1
        continue

    with open(file, "r") as f:
        lines = f.readlines()
        text = "\n".join(lines)

    line_idx = max(len(lines)//20, 150)
    start_and_end_lines = "\n".join(lines[:line_idx] + lines[-line_idx:])

    found = False
    for keyword in CC_KEYWORDS:
        if keyword in start_and_end_lines:
            found = True
            break
    
    if found:
        shutil.copy(file, output_path)
        copied_files += 1

    pbar.set_description(f"Copied {copied_files} files")
