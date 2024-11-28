# UK Hansard

This dataset contains the UK Hansard, which is the official report of all debates in the UK Parliament. The data is from the [Parlparse](https://parser.theyworkforyou.com/hansard.html).

## Downloading the Data

1. Clone the parlparse repo following the instructions in the link above. For example: `rsync data.theyworkforyou.com::parldata`. The required XML files are in the `scrapedxml` directory.
2. Use `python uk_to_dolma.py --base_folder <path scrapedxml from above> --output_folder <path to output folder> --shard_size 1` to convert the xml pages the dolma format.

```
