# Court Listener Data
Opinion data from CourtListener [bulk data list](https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/)

## Data download and processing
Run full processing including downloading the raw zipped data, unzipp to csv file and parsing to dolma format with
``bash get_data.sh``.

To test with only one zip file with ``bash get_data.sh --test_run 1``.

To change the maximum number of parallel jobs (8 by default) to run with ``--max_jobs``.
