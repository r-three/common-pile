# Biodiversity Heritage Library

A collection of public domain books from the [Biodiversity Heritage Library](https://www.biodiversitylibrary.org), a digital library for biodiversity literature.

# Data Download and Processing
1. Download metadata for all BHL titles with `bhl/get-metadata.sh`
2. Download the BHL content with `bhl/get-data.sh`. Note that newer versions of the BHL collection may be published in teh future and can be found on the [BHL data exports page](https://about.biodiversitylibrary.org/tools-and-services/developer-and-data-tools/#x--TXT)
3. Build an index of public domain titles with `python bhl/build-index.py`
4. Extract only the public domain titles from the collection with `python bhl/extract-files.py`
5. Convert collection to Dolma format with `python bhl/to-dolma.py`

Raw text and metadata will live in `data/biodiversity-heritage-library/raw` and processed text will live in `data/biodiversity-heritage-library/v0`

## Data Stats

| # Pages   | # Tokens |
|----------:|---------:|
|  42418499 |          |
