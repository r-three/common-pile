# Biodiversity Heritage Library

A collection of openly licensed educational materials (e.g., syllabi, lecture notes, problem sets, etc.) from [OERCommons](https://oercommons.org), an online platform for hosting open educational resources (OER).

# Data Download and Processing
1. Collect links to OERCommons resource pages by performing a search query for all English-language materials uploaded under Public Domain, CC BY, or CC BY-SA licenses with `python collect_search_results.py`.
2. Iterate through the resource page links and find the links to the raw content pages with `python find_content_links.py`.
3. Collect the content pages with `python get_content.py`.
4. Extract metadata and text content and write records to dolma with `python to_dolma.py`.


The final dolma dataset is saved to `data/oercommons`
