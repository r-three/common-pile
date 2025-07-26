# Common Pile Filtering

## Description
The config files in this directory were used to transform the [raw Common Pile v0.1 datasets](https://huggingface.co/collections/common-pile/common-pile-v01-raw-data-6826b454a5a6a445d0b51b37) into the [filtered Common Pile v0.1 datasets](https://huggingface.co/collections/common-pile/common-pile-v01-filtered-data-68300bb0a946d10dda697663), which are far cleaner and were ultimately used to train the [Comma v0.1 suite of models](https://huggingface.co/collections/common-pile/comma-v01-artifacts-68307f7adba7e59fa183fe78). 

## Usage
The filtering pipeline we use is entirely built within the [Dolma Toolkit](https://github.com/allenai/dolma) data cleaning/filtering framework. This framework consists of three main operations: tagging, deduplicating, and mixing. Tagging is the process of tagging examples or spans within examples with certain properties (e.g., length, quality score, perplexity, etc.). Deduplicating (in Dolma) is a special instance of tagging that tags examples that are duplicates of other previously seen examples. Mixing is the process of processing a dataset based on tagged attributes (e.g., filtering out examples based on example-level tagged attributes, replacing spans in examples based on span-level tagged attributes, etc.)

Below is an outline of how the config files in this directory were used to filter the Common Pile sources:
1. Taggers were run for each of the datasets using the `dolma tag` command. The exact taggers used for each source can be found by looking in a source's `mixer_configs/{source_name}.json` file and checking which attributes expected by that mixer. Note that some of the taggers are built-in Dolma taggers, while others are custom taggers that live in the `custom_taggers/` directory.
2. Global deduplication was run with the `dolma dedupe` command and the `dedupe_configs/global_dedupe.json` config file. This deduplication step removes approximate duplicates across all sources. Approximate duplicates are examples with >90% of their 20-grams in common.
3. Mixing was run with the `dolma mix` command and the `mixer_configs/{source_name}.json` configs for each source.
