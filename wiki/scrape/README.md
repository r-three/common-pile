# Wiki Scrapes
##

Going forward, large scale scraps should use the [wikiteam3](https://github.com/saveweb/wikiteam3) tools. There are multiple wiki formats (deku wiki, wikidot, etc.) that are detected by not implemented in that tool. If we want to collect those we will need custom code.

These tools are designed for getting data from mediawiki wikis that don't publish dumps.

Conversion to the dolma format can be used for any wiki that uses the `*-history.xml` format.

## Data Download

These steps are to be completed for each wiki that we are scraping.

1. Find all the namespaces that pages are listed under with `python get_namespaces.py --wiki ${wiki_url}`. This saves a mapping of namespace names to id's in `data/${wiki_name}/namespaces.json`.
2. Get all the pages under each namespace by following pagination links using `python list_pages.py --wiki ${wiki_url} -ns 0 -ns 1...`. The namespaces we want to scrape are generally:
  * `(Main)`: 0
  * `Talk`: 1
  * `UserTalk`: 3
Either the integer or the name can be used as input. This generates lists of page titles at `data/${wiki_name}/pages/${ns}.txt`.
3. Get the XML export of these pages with `python export_pages.py --wiki ${wiki_url}`. This get xml exports of the all the pages exported pages. It currently fetches all revisions so that we can build a complete author list. This will create a sharded xml export at `data/${wiki_name}/export/${shard_idx}-pages.xml`. The `<text>` tag contains the wikimedia markup.
4. Convert the XML export into the dolma format from the wiki directory with `python to-dolma.py --wiki ${wiki_url} --license ${license_str} --export ${path}`

The export format is the same as the wiki dump

Wiki archive scraps have ~3 versions, to use the same format as the dump and 1 has a unique format. Most of the wiki's
that aren't online anymore use this old format.
