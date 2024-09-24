# Wiki

## Data Generation

### WikiMedia

WikiMedia wikis are ones maintained by the WikiMedia Foundataion, they run things like wikipedia. They regularly publish dumps in the `-history.xml` format that we can convert to dolma files.

Insturctions for downloading and processing all the WikiMedia wikis can be found in the `dump/` directory.

### WikiTeam

The Internet Archive WikiTeam scrapes many wikis across the net and publishes them to the Internet Archive. Of these many are openly licensed. The insturctions for downloading and processing those can be found in the `archive/` directory.

### Wiki Scaping

We currently do not do wiki scraping, we just use published dumps or scrapes published by the Internet Archive. The `scrape/` directory has some tools to start scraping in the future. If we plan to do more scraping in the future, we should probably use [wikiteam3](https://github.com/saveweb/wikiteam3) instead of writing our own tools.

### Conversion to Plain Text

Following the README's in each subdirectory will result in dolma formatted files that are on-disk with wikitext versions as the `text` field. We then convert them to plain text.

1. Start the WTF Wikipedia parsing server using the instructions in the `parser/` directory.
2. Run `python preprocessing.py ...`
3. Run `python scripts/remove_html.py ...`

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

### Special Cases

Shout Wiki, WikiTravelAllLanguages
