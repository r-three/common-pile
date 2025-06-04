# Dolma CCCC

This readme documents how create the Dolma CCCC dataset.

We use the Dolma Toolkit to fetch and linearize WARC files from Common Crawl, as well as to filter by license, language, and quality.
We finish by deduplicating the data and creating a dataset.

## Setup

The pipeline was run from branch [`soldni/backoff`](https://github.com/ai2-llm/dolma/tree/soldni/backoff) of the Dolma repository.

Once the branch is merged, you can install the Dolma Toolkit with the following command:

```bash
# Install Dolma Toolkit
pip install 'dolma[warc,resiliparse]'
```

Full documentation for the Dolma Toolkit can be found at [github.com/allenai/dolma/docs](https://github.com/allenai/dolma/tree/main/docs).

## Fetching data

We use subcommand `dolma warc` to process WARC files from Common Crawl.
We use configuration as follows:

```yaml
# download list of warc files from Common Crawl (d.cache),
# read the file (d.file), split the file into lines (d.split),
# and add the bucket prefix by replacing the string "crawl-data"
# with "s3://commoncrawl/crawl-data" (d.sed)
documents: "${d.sed:${d.split:${d.file:${d.cache:https://data.commoncrawl.org/crawl-data/${oc.env:SNAPSHOT_ID}/warc.paths.gz}}},crawl-data@s3://commoncrawl/crawl-data@,@}"
destination:
    # save the documents in the S3 path; note that you
    # have to provide SNAPSHOT_ID as an environment variable
    - s3://ai2-llm/pretraining-data/sources/cccc/v0/documents/${oc.env:SNAPSHOT}
# use the number of processors available on the machine;
processes: ${d.procs:}
source_name: cccc_${oc.env:SNAPSHOT}
# we use the resiliparse linearizer to extract the text from the WARC files
linearizer: resiliparse

# pre-traggers work on the HTML content before linearization;
# that way we can extract license data using regular expression in cc_re_fast
# tagger, which looks for Creative Commons <div> tags.
pre:
    taggers:
        - cc_re_fast
    skip: true

# we do not store the HTML content, but keep 500 characters around the tagger
# content for manual inspection.
store:
    html: false
    attr_spans: 500

# we perform lightweight exact URL deduplication within each single WARC file,
# since duplicate URLs within a single WARC file are very likely to be the same document.
skip_duplicate_urls: true

# we process warc ifles in chunks of 100 documents at a time; this speeds up writing.
batch_size: 100

# location to save temporary work
work_dir:
  input: ${oc.env:HOME}/cccc/${oc.env:SNAPSHOT}/input
  output: ${oc.env:HOME}/cccc/${oc.env:SNAPSHOT}/output
```

Once the configuration above is saved to file (e.g. `cccc.yaml`), you can run the pipeline with the following command:

```bash
dolma -c cccc.yaml warc
```

The pipeline will process the WARC files and save the documents to the S3 path specified in the configuration.

## Language ID, Gopher and C4 rules filtering

The Dolma toolkit preprocesses content in two steps:
first, it tags documents according to rules or predictions from taggers (e.g., language);
then, predictions are used to filter full (or part of) documents.


We use the following command to run language ID, Gopher and C4 rules tagging:

```bash
export SNAPSHOT="CC-MAIN-2025-01"

dolma tag \
    --documents 's3://ai2-llm/pretraining-data/sources/cccc/v0/documents/${oc.env:SNAPSHOT}' \
    --taggers ft_lang_id_1e2 gopher_v2 tokenizer_repetitions_v2r2 c4_v2 \
    --processes 188
```

These taggers do the following:

- `ft_lang_id_1e2`: language ID using [FastText LID 176 model](https://fasttext.cc/docs/en/language-identification.html). Round scores to two decimal places. We use to filter to just web pages in English.
- `gopher_v2`: Gopher rules from [Massive Web text dataset](https://arxiv.org/abs/2112.11446v2). Identifies and remove documents that are low quality.
- `tokenizer_repetitions_v2r2`: Identify ngrams between 2 and 13 uniseg tokens that repeat more than 3 times. From [OLMo 2](https://arxiv.org/abs/2501.00656). These repeated sequences can lead to spikes during pretraining.
- `c4_v2`: C4 rules from [T5](https://arxiv.org/abs/1910.10683). Identifies and remove low quality documents.


After tagging, we filter the documents with the following configuration:

```yaml

streams:
  - name: cccc-CC-MAIN-2013-20
    documents:
          - s3://ai2-llm/pretraining-data/sources/cccc/v0/documents/${oc.env:SNAPSHOT}/*/warc/*/*.jsonl.zst
    attributes:
      - c4_v2
      - ft_lang_id_1e2
      - gopher_v2
      - tokenizer_repetitions_v2r2
    output:
      max_size_in_bytes: 2_000_000_000
      path: s3://ai2-llm/pretraining-data/sources/cccc/v1/documents/${oc.env:SNAPSHOT}
      min_text_length: 25   # in tokens
    filter:
      syntax: jq
      include:
        # Only English
        - >-
          (.attributes.ft_lang_id_1e2__ft_lang_id_1e2__en != null) and
          (.attributes.ft_lang_id_1e2__ft_lang_id_1e2__en[0][2] > 0.5)
      exclude:
        # C4 Rules
        - >-
          (.attributes.c4_v2__c4_v2__has_curly_brace != null) and
          (.attributes.c4_v2__c4_v2__has_curly_brace[0][2] > 0.5)
        - >-
          (.attributes.c4_v2__c4_v2__has_lorem_ipsum != null) and
          (.attributes.c4_v2__c4_v2__has_lorem_ipsum[0][2] > 0.5)
        - >-
          (.attributes.c4_v2__c4_v2__has_javascript != null) and
          (.attributes.c4_v2__c4_v2__has_javascript[0][2] > 0.5)

        # Gopher Rules
        - >-
          (.attributes.gopher_v2__gopher_v2__word_count != null) and
          (.attributes.gopher_v2__gopher_v2__word_count[0][2] < 50)
        - >-
          (.attributes.gopher_v2__gopher_v2__word_count != null) and
          (.attributes.gopher_v2__gopher_v2__word_count[0][2] > 100000)
        - >-
          (.attributes.gopher_v2__gopher_v2__median_word_length != null) and
          (.attributes.gopher_v2__gopher_v2__median_word_length[0][2] < 3)
        - >-
          (.attributes.gopher_v2__gopher_v2__median_word_length != null) and
          (.attributes.gopher_v2__gopher_v2__median_word_length[0][2] > 10)
        - >-
          (.attributes.gopher_v2__gopher_v2__symbol_to_word_ratio != null) and
          (.attributes.gopher_v2__gopher_v2__symbol_to_word_ratio[0][2] > 0.1)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_words_with_alpha_character != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_words_with_alpha_character[0][2] < 0.8)
        - >-
          (.attributes.gopher_v2__gopher_v2__required_word_count != null) and
          (.attributes.gopher_v2__gopher_v2__required_word_count[0][2] < 2)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_lines_starting_with_bullet_point != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_lines_starting_with_bullet_point[0][2] > 0.9)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_lines_ending_with_ellipsis != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_lines_ending_with_ellipsis[0][2] > 0.3)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_duplicate_lines != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_duplicate_lines[0][2] > 0.3)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_lines != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_lines[0][2] > 0.3)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_most_common_2gram != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_most_common_2gram[0][2] > 0.2)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_most_common_3gram != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_most_common_3gram[0][2] > 0.18)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_most_common_4gram != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_most_common_4gram[0][2] > 0.16)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_5grams != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_5grams[0][2] > 0.15)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_6grams != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_6grams[0][2] > 0.14)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_7grams != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_7grams[0][2] > 0.13)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_8grams != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_8grams[0][2] > 0.12)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_9grams != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_9grams[0][2] > 0.11)
        - >-
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_10grams != null) and
          (.attributes.gopher_v2__gopher_v2__fraction_of_characters_in_duplicate_10grams[0][2] > 0.10)

        # Remove documents with high repetition scores (over 32 repeated ngrams)
        - >-
          (.attributes.tokenizer_repetitions_v2r2__tokenizer_repetitions_v2r2__doc_max_score_repetition != null) and
          (.attributes.tokenizer_repetitions_v2r2__tokenizer_repetitions_v2r2__doc_max_score_repetition > 32)

work_dir:
  input: "${oc.env:HOME}/cccc/${oc.env:SNAPSHOT}/input"
  output: "${oc.env:HOME}/cccc/${oc.env:SNAPSHOT}/output"

processes: 188
```

we run this configuration with the following command:

```bash
dolma -c cccc.yaml mix
```

## Deduplicating data

We use Bloom filters to perform fuzzy deduplication of documents. We first identify duplicates as follows:

```yaml

documents:
    - s3://ai2-llm/pretraining-data/sources/cccc/v1/documents/${oc.env:SNAPSHOT}

dedupe:
  name: dedupe_para
  paragraphs:
    attribute_name: dedupe_para
    by_ngram:
      # consider all 20-grams of uniseg tokens in the document
      ngram_length: 20
      # with a stride of one, meaning that we move by 1 token and check if ngram is in bloom filter
      stride: 1
      # only keep annotations for paragraphs that have at least 50% of ngrams in the bloom filter
      overlap_threshold: 0.5
      # skip paragraphs that are less that 20 tokens
      skip_short_paragraphs: true
  skip_empty: true


bloom_filter:
  file: /tmp/dedupe-para/cccc-${oc.env:SNAPSHOT}.bloom
  read_only: false
  # the doc count is the estimated number of unique uniseg tokens in the snapshot
  estimated_doc_count: 50_000_000_000
  # a relatively high false positive for each ngram is okay, because we will check
  # all the ngrams in a paragraph before deciding if the paragraph is a duplicate.
  desired_false_positive_rate: 1e-02

processes: 188
work_dir:
  input: "${oc.env:HOME}/status/dedupe-para-v1/cccc-${oc.env:SNAPSHOT}/input"
  output: "${oc.env:HOME}/status/dedupe-para-v1/cccc-${oc.env:SNAPSHOT}/output"
```

We run this configuration with the following command:

```bash
dolma -c cccc.yaml dedupe
```

Like in the previous step, we filter out documents using the mixer

```yaml

streams:
  - name: cccc-CC-MAIN-2024-18
    documents:
      - s3://ai2-llm/pretraining-data/sources/cccc/v1/documents/${oc.env:SNAPSHOT}
    attributes:
      - dedupe_para
    output:
      max_size_in_bytes: 2_000_000_000
      path: s3://ai2-llm/pretraining-data/sources/cccc/v2/documents/${oc.env:SNAPSHOT}
      discard_fields:
        - attributes
    filter:
      syntax: jq
      exclude:
        # Fuzzy duplicates with above 0.8 of shared ngrams
        - >-
          (.attributes.dedupe_para | length > 0) and
          ((.attributes.dedupe_para | map(.[2] * (.[1] - .[0])) | add) / (.text | length) >= 0.8)

        # Remove NC or ND licensed pages
        - >-
          (.metadata.attribute_spans != null) and
          (.metadata.attribute_spans | keys | map(select(test("_nc_|_nd_"))) | length > 0)

work_dir:
  input: "${oc.env:HOME}/cccc/${oc.env:SNAPSHOT}/input"
  output: "${oc.env:HOME}/cccc/${oc.env:SNAPSHOT}/output"

processes: 188
```

Note we also take this opportunity to remove creative commons licensed documents with non-commercial (NC) clause, or documents that do not allow derivatives (ND).

We run this configuration with the following command:

```bash
dolma -c cccc.yaml mix
```
