# How to Contribute

## Discord channel

Coordination of this project is done primarily in the #licensed-pile channel in the [Eleuther AI Discord](https://discord.gg/eleutherai).

## Finding Data Sources

The goal of this project is to collect high-quality and diverse public domain or permissively licensed text. We track potential data sources in the repo's [Issues](https://github.com/r-three/licensed-pile/issues). If you find a new source of public domain or permissively licensed text that is not currently listed, feel free to add an Issue.

### Allowable Licenses

The current list of the permissive licenses allowed by this project is below and can also be found in `licensed-pile/licensed_pile/licenses.py`:

- [Public Domain](https://en.wikipedia.org/wiki/Public_domain_in_the_United_States)
- [Creative Commons Zero](https://creativecommons.org/publicdomain/zero/1.0/)
- [Creative Commons - Attribution 4.0](https://creativecommons.org/licenses/by/4.0/)
- [Creative Commons - Attribution 3.0](https://creativecommons.org/licenses/by/3.0/)
- [Creative Commons - Attribution Share-Alike 4.0](https://creativecommons.org/licenses/by-sa/4.0/)
- [Creative Commons - Attribution Share-Alike 3.0](https://creativecommons.org/licenses/by-sa/3.0/)
- [GNU Free Documentation License](https://www.gnu.org/licenses/fdl-1.3.en.html)
- [Apache 2 License](https://www.apache.org/licenses/LICENSE-2.0)
- [MIT License](https://opensource.org/license/mit/)
- [BSD License](https://opensource.org/license/bsd-2-clause/)

This list contains some of the common permissive licenses that cover many large data sources, but we intend to expand this list as we continue to collect data. If you come across a source with a license that you believe should be on this list, feel free to comment in our [Allowable License Meta-Issue](https://github.com/r-three/licensed-pile/issues/34). 

### Finding License Information

License information can sometimes be difficult to find for certain text sources on the Internet. If you are having trouble identifying the license or status of a data source, feel free to ask in the Discord channel. Below are a couple common patterns you can use to search for whether a source is permissively licensed or in the public domain:

1. A website's Terms of Service

> For example, Section 6 of the the [StackExchange Terms of Service](https://stackoverflow.com/legal/terms-of-service/public#licensing) state that their content is under a CC-BY-SA.

2. A license statement sometimes located at the bottom of a webpage

> For example, the [Pokemon Fandom Wiki](https://pokemon.fandom.com/wiki/Pokémon_Wiki) states near the bottom of their homepage *"Community content is available under CC-BY-SA unless otherwise noted"*

3. Metadata associated with individual items in a collection

> For example, [this book](https://www.biodiversitylibrary.org/part/156123) from the Biodiversity Heritage Library has metadata indicating that it is available under a CC-BY-NC license. Item-level license metadata is typically found in digital libraries and collections.

4. Sources in the public domain by U.S. law

> Certain types of content are automatically in the public domain by U.S. law. Some examples are works published over 100 years ago whose copyright has expired (e.g., many of the books on [Project Gutenberg](https://www.gutenberg.org)) and works authored by the U.S. Federal Government.

5. An "about" page can include licensing information for the website as a whole.

## Contributing Data Collection Code 

Once you have selected a source from the list of [Issues](https://github.com/r-three/licensed-pile/issues), add a comment that you plan to work on it and an adim will assign the issue to you. Then, you can follow these guidelines for how to get started with contributing to the repo:

1. Clone the repo

2. Run `pip install -r requirements.txt`
    
3. Create a subdirectory for your data source (e.g., the `licensed-pile/gutenberg` directory for the Project Gutenberg data source).

4. Identify the best way to collect the raw data

> Many sources provide a bulk data download for downloading all of their data at once. If this option is not available, check if the source provides a developer API for downloading data programmatically. If neither of these are provided then consider scraping the data from the webpage directly as long as scraping is allowed in the source's Terms of Service.

4. Write a script to download the raw data and store this in `licensed-pile/data/{SOURCE}/raw`

> The convention used in this repository is that scripts are run from the top-level `licensed-pile/` directory using `python -m {SOURCE}/{SCRIPT_NAME}`. Thus, your script should write to the relative path `./data/{SOURCE}/raw`.

5. If necessary, write code to filter the downloaded items down to only those with appropriate licenses.

6. Write code that outputs the resulting data to `licensed-pile/data/{SOURCE}/v0` 

> The data format used in this project is [Dolma](https://github.com/allenai/dolma). To write out the resulting data as a Dolma dataset, convert each record in the dataset to a python dictionary and use the utilities in `licensed-pile/licensed_pile/write.py` to convert the list of dictionaries to a Dolma dataset. In cases where the dataset is very large, it is better to define a record generator rather than a list and pass the generator to the Dolma utility functions.

> Each record should minimally have the following keys:  
```json
{
    "id": <unique record identifier>,
    "text": <record text>,
    "source": <name of source>,
    "added": <date/time when record was added to the dataset>,
    "metadata": {
        "license": <license string>,
    }
}
```

For some sources, significant compute may be required to collect all of the data. For these, it is fine to test code on a subset of the data. Ultimately, we plan on re-running the code for all data sources all at once after all the code has been written in order to get the freshest version of each source.

## Global Data Formatting Guidelines

1. All mathematics should be formatted as compilable LaTeX. This doesn't necessarily aply to individual numbers, but once something is an equation or requires special formatting to make it appear correctly write it in LaTeX. Note that for some sources (notably wikipedia) this means we need to convert their mathematics syntax to LaTeX.

## Examples

For some examples of completed data sources check out any of the following:
- `licensed-pile/gutenberg`
- `licensed-pile/bhl`
- `licensed-pile/stackexchange`
- `licensed-pile/ubuntu`
