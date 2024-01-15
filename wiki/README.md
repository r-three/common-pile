# Wiki

## Notes

The following scanners output a .history.xml to parse
* "Internet Archive HTML5 Uploader ...": Seems to have .7z
*  "wikiteam3 (v...)" these get released as .zstandard files.
* Official Wikipedia Dumps
* "Internet Archive Python library ..." >= 1.0.4


The following use the old format
* "Internet Archive Python library 0.X.X": As a zip file, you need to make a new dir with -d when you unzip.


The archive url can be created with `f"archive.org/details/{item_id}"`


Some of the items have multiple uploads, for example  `wiki-kris159shoutwikicom_w` has multiple history files we so need to parse out the date and pic the most recent one, i.e., `kris159shoutwikicom_w-20180506-history-xml.7z` over `kris159shoutwikicom_w-20140129-history.xml.7z`

## Special Cases

Shout Wiki, WikiTravelAllLanguages
