# Foodista

This Code scrapes the Foodista shared recipe site which is licensed under CC-BY-3.0

## Downloading the Data

1. Use `python build_index.py` to get a list of pages on the site by parsing the sitemap.
2. Use `python download_pages.py` to download the pages. `--wait` can be used to give the remote server a break between requests. `--num_threads` controls how many threads are used to download the data. This script can do incremental downloads, or it can re-download everything with the `--overwrite` flag.
3. Use `python to_dolma.py` to convert the pages from files on disk to the dolma format. Each page is raw html at this point.
4. Use `python preprocess.py` to parse the html into plain text. This uses dolma for multiprocessing of the various data shards.

You can also use `get-data.sh` to do all the steps above automatically.

Note: It is normal to see messages in the log like this when doing test runs, but it would be concerning when running the real data collection.

``` json
{"level_name": "ERROR", "timestamp": "2024-05-07 14:02:52,846", "module_name": "to_dolma", "function_name": "format_page", "logger": "food", "message": "Article data/pages/foodista.com_tool_Z2MHM8QR_julienne-peeler.html exists in the index but is not downloaded."}
```

## Timing

Foodista isn't the most robust site, hitting it with parallelism or with a wait less than 5 seconds between requests
results in server errors.

There are ~70K pages, with a 5 seconds wait, it will take ~4.3 days to scrape the entire site.
