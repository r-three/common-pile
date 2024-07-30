# MediaWiki

## Steps:

1.  Run `download.sh YYYYMMDD` to download xml dumps
2. Run `to_dolma.sh YYYYMMDD` (date must match) to convert to the dolma format
3. Run `python preprocess.py --input ... --output ...`
