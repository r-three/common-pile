# PressBooks

A collection of openly licensed textbooks from [PressBooks](https://pressbooks.com), a repository of OER textbooks.

# Data Download and Processing
1. Collect links to all books in the catalog with `python scrape_search_results.py`.
2. Process search results with `python process_search_results.py`.
3. Collect contents of each book with `python scrape_books.py`.
4. Remove duplicate books with `python remove_duplicate_books.py`.
5. Extract text content and metadata and write records to dolma with `python to_dolma.py`.

The final dolma dataset is saved to `data/pressbooks`
