# GitHub Archive Threads

This data source is a collection of threads (issues and pull-request plus comments) from github repositories with open
source licenses (as these comments inherit the license of the repo).

## Steps

1. Run the `threads.sql` query in Google Big Query.
  * You will need to set a "destination table" to store the result. This can be done from the "more > query settings"
    menu. Make sure to check the box that allows for large results.
2. Export the data from a Big Query table to a cloud bucket.
  * Select the table to created above to open up the schema explore for the table. There will be an "export" menu. Use
    this to pick your output bucket, pick the JSON (newline delimited) format, and pick gzip for compression.
3. Download the exported data to `./data/bigqurey` with `gcloud storage cp gs://${bucket_name}/{export_name}*
   ./data/bigquery`
4. Run `to_dolma.py`
5. Run `clean_dolma.py`
