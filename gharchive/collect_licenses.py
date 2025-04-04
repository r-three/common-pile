import json

from tqdm import tqdm

licenses = set()
with open(
    "/fruitbasket/users/bdlester/projects/licensed_pile/gharchive/data/licenses.jsonl"
) as f:
    for line in tqdm(f):
        if line:
            data = json.loads(line)
            for license in data["license"]["licenses"]:
                licenses.add(license["license"])


with open(
    "/fruitbasket/users/bdlester/projects/licensed_pile/gharchive/data/all_licenses.json",
    "w",
) as wf:
    json.dump(list(licenses), wf)
