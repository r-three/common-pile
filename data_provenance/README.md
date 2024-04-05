# Processing scripts for Data Provenance data

The [Data Provenance Initiative](https://www.dataprovenance.org) is a digital library for supervised datasets that have been manually annotated with their source and license information. It wraps HuggingFace datasets with extra metadata, and provides code to download, standardize and filter for various criteria. 

In this case, we have filtered for the following criteria:
* English language or code data
* Datasets have a commercially viable license, found through the Data Provenance Initiative or the hosting GitHub repository
* The original source(s) of the text are (a) not machine generated, and (b) only from the following list of accepted sources:

```
["crowdsourced", "crowdsourced (amt)", "wordnet", "crowdsourced (daemo)", "human", "grammar-based",
"wikipedia.org", "commoncrawl.org", "stackexchange.com", "github", "opus news-commentary"
"wikihow.com", "dbpedia", "verbnet", "creative commons license textbooks", "pubmed articles", "pubmed"
"project gutenberg", "eur-lex portal", "arc corpus", "wikisource", "globalvoices.org", "opus dogc corpus",
"conceptnet", "semeval 2012 task 2", "crowdflower.com", "yahoo! answers"
"wiktionary.org", "winograd schema challenge dataset", "sec.gov/edgar/about", "aclanthology.org"
"parallel meaning bank", "headlines", "freebase", "eu legistlative texts", "search-engine(google) auto-complete",
"wikidata",]
```

Here is the process to download the data:

1. Run `python download.py`
