import json

from flask import Flask, request
from tqdm import tqdm

app = Flask(__name__)

LICENSES = {}
with open(
    "/fruitbasket/users/bdlester/projects/licensed_pile/gharchive/data/licenses.jsonl"
) as f:
    for line in tqdm(f):
        if line:
            data = json.loads(line)
            LICENSES[data["repo"]] = data["license"]


@app.route("/license", methods=["POST"])
def get_license():
    repo = request.json["repo"]
    return LICENSES.get(repo, {})
