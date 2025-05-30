# ArXiv

The latex parsing is difficult, with many libraries failing to fully parsing the latex in various different ways. Thus
we have decided to follow the arxiv latex -> html pipeline instead. This way as they improve that we will get the
benefits.

## Data Download

1. Setup credentials for boto3 as the arxiv s3 buckets are requester pays.
2. Run `python bulk_download.py --download_old --download_manifest`
3. Run `extract.sh`
4. Download metadata from https://www.kaggle.com/datasets/Cornell-University/arxiv. We use this metadate over a manual scrape of the oai2 end point with a tool like metha as the kaggle dump has license information as a field instead of trying to parse it out of a `<dc:description>` field. The Kaggle dump is updated ~weekly, for example on 2024/01/13, the most recent paper was from 2024/01/05.
5. Extract the downloaded metadata `unzip arxiv-metadata-oai-snapshot.json.zip`
6. Run `python to-dolma.py`. This will download required bulk download shards as needed. As only ~15% of the articles are CC licensed, there is a lot of possible saving by not pre-downloading everything.
7. Run `python preprocess.py`. This will parse the latex into more readable plain text. The math is left as it. Currently section and citation references are not handled that well.

#### Test Run

To do a test run, download a subset of data with `python bulk_download.py --download_manifest --test_run` and then run later commands with `--dry_run` so extra shards aren't downloaded.

### All Licenses

``` sh
$ cat data/arxiv-metadata-oai-snapshot.json | jq -s .[].license | sort | uniq
"http://arxiv.org/licenses/nonexclusive-distrib/1.0/"
"http://creativecommons.org/licenses/by/3.0/"
"http://creativecommons.org/licenses/by/4.0/"
"http://creativecommons.org/licenses/by-nc-nd/4.0/"
"http://creativecommons.org/licenses/by-nc-sa/3.0/"
"http://creativecommons.org/licenses/by-nc-sa/4.0/"
"http://creativecommons.org/licenses/by-sa/4.0/"
"http://creativecommons.org/licenses/publicdomain/"
"http://creativecommons.org/publicdomain/zero/1.0/"
null
```
