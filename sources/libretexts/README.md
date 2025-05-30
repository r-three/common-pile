# LibreTexts

A collection of openly licensed textbooks from [LibreTexts](https://libretexts.org), a repository of OER textbooks.

# Data Download and Processing
1. Collect links to all books in the LibreTexts catalog with `python scrape_search_results.py`
2. Visit each book and collect links to each of their sections with `python scrape_section_links.py`.
3. Visit each book section and collect its contents with `python scrape_section_contents.py`.
4. Extract text content and metadata and write records to dolma with `python to_dolma.py`.

The final dolma dataset is saved to `data/libretexts`
