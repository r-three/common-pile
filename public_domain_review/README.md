# Public Domain Review

A collection of CC BY-SA essays from the [Public Domain Review](https://publicdomainreview.org), an online publication that publishes essays on works of art and literature that have entered the Public Domain.

# Data Collection

To collect the CC essays published on PDR, run the script `public_domain_review/run.sh`. Internally, this will run the `scrape-collections.py`, `scrape-essays.py`, and `scrape-conjectures.py` python scripts that extract the text from PDR's [collections](https://publicdomainreview.org/collections/), [essays](https://publicdomainreview.org/essays/), and [conjectures](https://publicdomainreview.org/conjectures/) series. This command will save three Dolma datasets in `data/public-domain-review/v0`.
