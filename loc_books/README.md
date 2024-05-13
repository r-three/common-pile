# Library of Congress Selected Digitized Books

Public domain books digitized by the US Library of Congress (LoC) as part of the collection [Selected Digitized Books](https://www.loc.gov/collections/selected-digitized-books/about-this-collection/).

## Data Stats

| # Books | # Tokens | Size (Raw) | Size (Dolma)
|--------:|---------:|-----------:|-----------:|
|  135,500 | ~ 8 billion | ~ 50 GB | ~20 GB |

## Data download & export

The source uses two Python CLI scripts for downloading and exporting metadata and books: `metadata.py` and `books.py`. The scripts need to be run inside the `loc_books` directory.

These scripts both offer the same two commands: `download` and `export`. You can get help for both commands using the `--help` flag. The `download` command is resumable, so if there are connection issues and a download is restarted, the scripts will  skip previously downloaded files and not re-download them from the server.

### 1. Install dependencies

Install the pip dependencies for this source using `pip install -r requirements.txt`.

### 2. Download the metadata

> To download a metadata snapshot you need to choose a name for the snapshot. The snapshot name will be used by the scripts to consistently label downloads and exports. The recommended format for a snapshot name is the current date in ISO format ("YYYY-mm-dd"), e.g. `2024-05-13`. This format makes the snapshots easy to sort by date. However, this format is not required and it is also possible to use an arbitrary label such as `latest` as the snapshot name.

Run the script by supplying the snapshot name using the flag `--snapshot`:

`python metadata.py download --snapshot 2024-05-13`

The metadata download should take ~30 minutes due to [API limits](#api-limits). The script downloads the metadata page by page as JSON files divided into subfolder by publication date range in order to avoid [deep paging](#deep-paging) issues. To make sure that each page was downloaded you can simply run the script again and any missing pages will be downloaded and existing files skipped.

### 3. Export the metadata

Export the metadata to CSV using the export command by supplying the snapshot name using the flag `--snapshot`:

`python metadata.py export --snapshot 2024-05-13`

The resulting CSV file will be saved to the path `data/exports/metadata` with the filename `"{snapshot_name}.csv"`, e.g. `2024-05-13.csv`.

### 4. Download the books

Start the book download by supplying the snapshot name using the flag `--snapshot`:

`python books.py download --snapshot 2024-05-13`

The download is parallelized and should take around 12-24 hours. Minor connection issues are handled by the script by retrying each URL three times in case of a HTTP error. If you run into major connection or memory issues simply abort the script and run it again, the download will resume where it stopped.

### 5. Export the books

After downloading the books you can export them to the dolma format by supplying the snapshot name using the flag `--snapshot`:

`python books.py export --snapshot 2024-05-13`

## Folder structure

The metadata and the book texts are downloaded and exported to the folder `./data` according to the following folder structure:
```
data
└───downloads
│   └───books
|   |   |   1ersalndeartec00sal_djvu.txt
|   |   |   1ourcountryitspe00brec_djvu.txt
|   |   │   ...
│   └───metadata
│       └───2024-05-13
|       |   └───2000-2025
|       |       |   0001.json
|       |       |   0002.json
|       |       |   ...
|       |   └───1900-1999
|       |   └───...
│       └───2024-05-06
│       └───...
│
└───exports
│   └───books
│   |   └───2024-05-13
|   |   |   |   00000_loc_books.jsonl.gz
|   |   |   |   00001_loc_books.jsonl.gz
|   |   │   |   ...
│   └───metadata
│       |   2024-05-13.csv
│       |   2024-05-06.csv
│       |   ...
```

## API limits

### Metadata

The metadata is fetched through the LoC JSON API. The [API applies rate limits](https://www.loc.gov/apis/json-and-yaml/working-within-limits/) of **20 requests per 10 seconds and 80 requests per minute** for the collection endpoint, so this code uses **10 requests per 10 seconds and 60 requests per minute** to be on the safe side and avoid HTTP 429 errors.

#### Deep paging

The API uses pagination which increases memory usage with each page and stalls the server beyond 100,000 results. To decrease the server load caused by [deep paging](https://www.loc.gov/apis/json-and-yaml/working-within-limits/#deep-paging), the script sends requests based on the date facets suggested in the first server response and downloads the metadata subsets based on date ranges such as `1800-1899` or `1900-1999`.

### Books

The books are not downloaded through the API, but through the separate download server `tiles.loc.gov`. This download server does not have official rate limits, so the script uses its own rate limits based on experience: **10 requests per second, 80 requests per 10 seconds and 400 requests per minute**.
