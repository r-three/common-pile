# MediaWiki

These are tools that are used to download wiki data from offical dumps distributed by the MediaWiki foundataion.

The downloading tools are bespoke for the MediaWiki naming scheme, but the conversion to dolma script can be used for other wikis that share the `*-history.xml` dump format.

## Steps:

1. Run `download.sh YYYYMMDD` to download xml dumps
2. Run `to_dolma.sh YYYYMMDD` (date must match) to convert to the dolma format

This results on dolma formatted data on disk with wikitext. Use the shard wikitext preprocessing pipeline to get plaintext.
