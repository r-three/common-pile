# USPTP

USPTO dataset extracted from [Google Patents Public Dataset](https://cloud.google.com/blog/topics/public-datasets/google-patents-public-datasets-connecting-public-paid-and-private-patent-data) and uploaded to HF.

## Data Download and Processing

To clone the unprocessed dataset from HuggingFace run `bash setup.sh`. The default location is `/uspto/data`

`pandoc` is required to run the script. The command to install it is provided in the script (commented out). Alternatively you can install it with`sudo apt-get install pandoc` but that installs an older version.


The main script can be run with `bash run process_uspto.sh --output-dir <output_dir> --max-concurrency <int> --limit <max_rows>`.

Note: The script will take a long time to run. The `--max-concurrency` flag can be used to speed up the process. The `--limit` flag can be used to limit the number of rows processed.
It takes ~30 mins to process 1 file with 256 threads. The bulk of the processing is done by pandoc.

To save the processed data to parquet add the `--to-parquet` flag.

<details>
<summary>Under the hood of process_uspto.sh</summary>

### setup.sh has 3 main steps:

#### Usage
1. Ensure you are in the correct directory structure:
    1. The script expects to be run from the parent directory of the `uspto` directory.

#### Running the Script:
- Make sure the script has execute permissions. If not, run:
    ```sh
    chmod +x process_uspto.sh
    ```

#### It has the following steps:
1. The main bulk of the processing in the python script are the pandoc conversions. A progress bar is displayed for each column/file.

</details>


## Data Stats


## Example
Some output examples are in the examples dir.

## License
Creative Commons - Attribution - https://creativecommons.org/licenses/by/4.0/
