# ArXiv
##

## Data Download

1. Setup credentials for boto3 as the arxiv s3 buckets are requester pays.
2. Run `bulk_download.py`
3. Run `extract.sh`
4. Download metadata from https://www.kaggle.com/datasets/Cornell-University/arxiv. We use this metadate over a manual scrape of the oai2 end point with a tool like metha as the kaggle dump has license information as a field instead of trying to parse it out of a `<dc:description>` field. The Kaggle dump is updated ~weekly, for example on 2024/01/13, the most recent paper was from 2024/01/05.
5. Extract the downloaded metadata `unzip arxiv-metadata-oai-snapshot.json.zip`
