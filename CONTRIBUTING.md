# How to Contribute

## Finding Data Sources

This project is interested in collecting data that is in the public domain or under a permissive license such as Creative Commons, GFDL, MIT, Apache or BSD licenses. A list of acceptable licenses is below and can be found in `licensed-pile/licensed_pile/licenses.py`:

- Public Domain
- Creative Commons Zero - https://creativecommons.org/publicdomain/zero/1.0/
- Creative Commons - Attribution - https://creativecommons.org/licenses/by/4.0/
- Creative Commons - Attribution - https://creativecommons.org/licenses/by/3.0/
- Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/4.0/
- Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/3.0/
- GNU Free Documentation License
- Apache 2 License - https://www.apache.org/licenses/LICENSE-2.0
- MIT License
- BSD License

Typically, license information can be found in a source's Terms of Use. For a list of potential sources we plan to are in process of collecting from, see the [Issues](https://github.com/r-three/licensed-pile/issues).

## Collecting Data

Once you have selected a source from the list of [Issues](https://github.com/r-three/licensed-pile/issues) and assigned that issue to yourself, you can follow these guidelines for how to get started with contributing to the repo:

1. Clone the repo.
2. Run `pip install -r requirements.txt`.
3. Create a subdirectory for your data source (e.g., the `gutenberg` top-level directory for the Project Gutenberg data source).
4. Write a script to download the raw data and put this in `licensed-pile/data/{SOURCE}/raw`. Often sources will provide a bulk data download for getting all their data at once. If this is not available, consider using an API provided by the source or scraping the data if that is allowed in their Terms of Service.
5. Filter the downloaded items down to only those with appropriate licenses.
6. Write the filtered data using the [Dolma](https://github.com/allenai/dolma) data format to `licensed-pile/data/{SOURCE}/v0`. Shared utilities for converting lists (or generators) of dictionaries to Dolma are found in `licensed-pile/licensed_pile/write.py`.

For some data sources, significant compute may be required to collect all of the data. For these, it is fine to write and commit code tested on a subset of the data. Ultimately, we plan on re-running the code for all data sources at once at the end of this project.
