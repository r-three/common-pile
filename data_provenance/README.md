# Processing scripts for Data Provenance data

The [Data Provenance Initiative](https://www.dataprovenance.org) is a digital library for supervised datasets that have been manually annotated with their source and license information. It wraps HuggingFace datasets with extra metadata, and provides code to download, standardize and filter for various criteria.

In this case, we have filtered for the following criteria:
* English language or code data
* No model generated text
* Datasets have a commercially viable license, found through the Data Provenance Initiative or the hosting GitHub repository
* We only include datasets where all associated licenses (from the Data Provenance Initiative and GitHub) are open source compliant or appear in the Gold, Silver or Bronze lists of the Blue Oak Council (https://blueoakcouncil.org/list).
* The original source(s) of the text are only from the list of sources in `source_allow_list.txt`
* We only include datasets where the relevant license sources are thoroughly documented and linked.

The specific filter settings are here: https://github.com/Data-Provenance-Initiative/Data-Provenance-Collection/blob/main/src/configs/pile_v2_test.yaml


Here is the process to download the data, from inside the `data_provenance` dir:

1. Run `python3 hf_downloader.py`

2. Run `python3 to-dolma.py --include include.csv`
