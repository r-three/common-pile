# USPTP

USPTO dataset extracted from [Google Patents Public Dataset](https://cloud.google.com/blog/topics/public-datasets/google-patents-public-datasets-connecting-public-paid-and-private-patent-data) and uploaded to HF.

## Data Download and Processing

The script uses a local API to convert the MATHML equations to LaTeX. To download the dataset and install the code necssary to install the server, run `bash setup.sh`.
<details>
<summary>Under the hood of run.sh</summary>
setup.sh has 3 main steps:

1. Clones the dataset from Huggingface
2. Clone the MathML to LaTeX server
3. Compiles the TypeScript code.
</details>

The main script can be run with `bash run process_uspto.sh --output_dir <output_dir> --max_concurrency <int> --limit <max_rows>`

<details>
<summary>Under the hood of process_uspto.sh</summary>

### setup.sh has 3 main steps:

#### Usage
1. Ensure you are in the correct directory structure:
    1. The script expects to be run from the parent directory of the `uspto` directory.
    2. Inside the `uspto` directory, there should be a `mathml-to-latex` directory with the Node.js server script.

#### Running the Script:
- Make sure the script has execute permissions. If not, run:
    ```sh
    chmod +x process_uspto.sh
    ```

#### It has the following steps:
1. Checks if we are in the `uspto` directory.
2. Starts the MathML to LaTeX server.
3. Runs the Python script to process the parquet files.
4. Cleans up after the process is finished.

</details>


## Data Stats


## Example
Some output examples are in the examples dir.
