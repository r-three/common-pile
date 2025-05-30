# Scripts

## Stats

This script can be used with `size-stats-dolma` once the common-pile library is installed. It takes an `--input` that points to some dolma formatted data (a file, a glob, a dir) and outputs statistics about the size of the data.

Example:

```
characters: 447Mc [00:09, 48.3Mc/s]
bytes: 448Mb [00:09, 48.4Mb/s]kd/s]
tokens: 71.5Mt [00:09, 7.73Mt/s]
documents: 20.3kd [00:09, 2.19kd/s]
shards: 1.00s [00:09, 9.25s/s]Mc/s]
```

Note: It uses SI units (kd => thousand documents, Mt => Mega tokens, a million tokens, etc.) and you can get trailing uglyness from tqdm bars that aren't totally deleted (the `Mc/s]` at the end, etc.)

Characters is the number of characters in the string according to python (`len(example["text"])` ~ the number of unicode code points). Bytes is the number of utf-8 bytes in the string (`len(example["text"].encode("utf-8"))`)

## Compare Data

This is a tool that can be useful for spot checking errors and looking for patterns that could be cleaned up during text preprocessing. It shows the difference between examples at different stages of a dolma pipeline,

1. Install streamlit `pip install streamlit`
2. Run with `streamlit run compare_data.py`
3. Fill in the paths to load the data. It will take a bit, but after that the data will be cached for the whole streamlit session.
4. Use the controls to look around at different example to see the differences between them at different pre-processing steps.
