# USGPO

Government documents published by the [US Government Publishing Office](http://www.gpo.gov). Since each of these documents are authored by the US Federal Government, they are in the Public Domain.

# Data Collection

To collect the documents, run the script `usgpo/get-data.sh` from the repo's top-level directory. Internally, this will run `get-links.py` to get a collection of links to the government documents and `download-files.py` to download each link and parse out the relevant text. This command will save the final dataset in `data/usgpo/v0`.

