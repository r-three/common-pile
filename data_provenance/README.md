# Processing scripts for Data Provenance data

The [Data Provenance Initiative](https://www.dataprovenance.org) is a digital library for supervised datasets that have been manually annotated with their source and license information. It wraps HuggingFace datasets with extra metadata, and provides code to download, standardize and filter for various criteria.

In this case, we have filtered for the following criteria:
* English language or code data
* No model generated text
* Datasets have a commercially viable license, found through the Data Provenance Initiative or the hosting GitHub repository
* The original source(s) of the text are only from the list of sources in `source_allow_list.txt`

The specific filter settings are here: https://github.com/Data-Provenance-Initiative/Data-Provenance-Collection/blob/main/src/configs/pile_v2_test.yaml


Here is the process to download the data:

1. Run `PYTHONPATH=. python data_provenance/download.py --include data_provenance/include.csv`

2. Run `PYTHONPATH=. python data_provenance/to-dolma.py --include data_provenance/include.csv`
