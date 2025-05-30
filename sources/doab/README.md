# Directory of Open Access Books

A collection of public domain and openly licensed books from the [Directory of Open Access Books](https://www.doabooks.org/en).

# Data Download and Processing
1. Download metadata for all DOAB titles from DOAB's [metadata harvesting page](https://www.doabooks.org/en/resources/metadata-harvesting-and-content-dissemination).
2. Download the PDFs for English-language books under CC BY and CC BY-SA licenses with `python download_books.py <path_to_metadata> data/doab/raw`.
3. The steps up until now will result in some in some downloaded files being HTML for a book's landing page rather than the book PDF itself. To parse these HTML files and attempt to download the PDF, run `python download_additional_books.py <path_to_metadata> <glob_to_pdfs> data/doab/raw_additional`.
4. Convert the downloaded PDFs to plaintext (note: this step requires installing [Marker](https://github.com/VikParuchuri/marker)) with `python convert_pdfs_parallel.py --input-glob <glob_to_pdfs> --output-directory <path_to_output_directory>`.
5. Perform an additional validation step that inspects each book's plaintext and keeps only the ones containing an open license statement with `python cc_filter.py --input-files <input_files> --output_dir <path_to_output_directory>`.
6. Convert the final plaintext files to a dolma dataset with `python to_dolma.py --metadata <path_to_metadata> --input-files <input_plaintext_files> --output-dir data/doab/v0`

After this process, raw data will be in `data/doab/raw` and the processed dolma dataset will be in `data/doab/v0`.

