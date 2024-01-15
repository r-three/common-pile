#!/usr/bin/env python3
import collections
import glob
import json

import tqdm
from smart_open import smart_open

import wiki

template_counts = collections.Counter()
for i, f in enumerate(
    glob.glob(
        "/media/brian/External-SSD/licensed_pile/wiki/data/wiki/dump/raw/documents/*_wikipedia.com.jsonl.gz"
    )
):
    print(f"Extracting math templates from {f}")
    with smart_open(f) as f:
        for l in tqdm.tqdm(f):
            if not l:
                continue
            data = json.loads(l)
            if not data["text"]:
                continue
            _, templates = wiki.extract_math_templates(data["text"])
            for t in templates:
                template_counts[t] += 1
print(f"{len(template_counts)} unique math templates found.")
print(f"{sum(template_counts.values())} total math templates found.")
with open("math_templates.json", "w") as wf:
    wf.write(json.dumps(dict(template_counts)))
