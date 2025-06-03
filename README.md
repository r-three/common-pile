# The Common Pile

This repository tracks the code used to collect, process, and prepare the datasets for the Common Pile.
The code used for the preparation of each source in the Common Pile can be found in the `sources/` subdirectory.
Source-agnostic utility code and scripts are provided in the `common_pile` package.
If you are looking for the data itself or our trained models, please see [our Hugging Face organization](https://huggingface.co/common-pile/).

## Installation

The majority of packages required for dataset creation can be installed with `pip install -r requirements.txt`.
To make use of the shared functionality in the `common_pile` pckage, run `pip install -e .`.
If you are on a system that doesn't support automatic installation of pandoc with `pypandoc_binary`, change it to `pypandoc` in the `requirements.txt` and and install pandoc manually.

## Contributing

If you'd like to contribute a new source to the Common Pile, please [start an issue](https://github.com/r-three/common-pile/issues/new) to share details of the source.
Generally, we expect each source to include code that 1) downloads the data, 2) processes it appropriately to retain primarily plain text, and 3) write out the results in the Dolma format (gzipped jsonl).
You can find utilities to help with each of these steps in the `common_pile` library.
Alternatively, you can look at our existing sources for ideas as to how to prepare a source.
We use git pre-commit hooks to format code and keep style consistent.
You can install the pre-commit libraries with `pip install pre-commit` and insert the pre-commit hooks with `pre-commit install` from the repository root.

## Tips

The [scripts subdirectory](https://github.com/r-three/common-pile/tree/main/common_pile/scripts) has various scripts that can be helpful for inspecting or computing statistics over data.
Alternatively, the Dolma-formatted files can be inspected with [`jq`](https://jqlang.org/) by running

```
cat ${file}.jsonl.gz | gunzip | jq -s ${commmand}
```
