# PubMed Central

A collection of journal articles from [PubMed Central](https://www.ncbi.nlm.nih.gov/pmc/), "a free full-text archive of biomedical and life sciences journal literature at the U.S. National Institutes of Health's National Library of Medicine (NIH/NLM)."

## Data Download and Processing

Downloading and processing code is easy. Simply use `bash run.sh` (or `bash run.sh 1000` for debugging with 1000 samples)

<details>
<summary>Under the hood of run.sh</summary>
Run.sh has 3 main steps:

1. Download the list of all articles with `bash get-filelist.sh`
2. Download the data and convert from nxml to markdown with `bash download-and-convert-to-md.sh`
3. Convert the data to the Dolma format with `python to-dolma.py`
</details>

Files converted to markdown will live in `data/md`, author lists live in `data/authors`, and processed files will live in `data/pubmedcentral`

## Data Stats

| # Articles | # Tokens |
| ---------: | -------: |
|    3997890 |          |

## Example

``` json
{
    "id": "'PMC176545'",
    "text": "Introduction {#s1}\n============\n\nHuman malaria is caused by four species of the parasitic protozoan ...",
    "source": "PubMed Central",
    "added": "2024-02-01T17:02:52.985753",
    "metadata": {
        "license": "Creative Commons - Attribution - https://creativecommons.org/licenses/by/4.0/",
        "url": "https://www.ncbi.nlm.nih.gov/pmc/articles/PMC176545/",
        "journal": "PLoS Biol. 2003 Oct 18; 1(1):e5",
        "authors": "Bozdech, Zbynek and Llinás, Manuel and Pulliam, Brian Lee and Wong, Edith D and Zhu, Jingchun and DeRisi, Joseph L",
    }
}
```

## Notes
Converting documents from nxml to markdown requires the pandoc library, which can be installed following the instructions on the [pandoc website](https://pandoc.org/installing.html).


TODO:
- [ ] Confirm article and token #s, fill in example
- [ ] Handle references to figures and tables
- [ ] Handle citations
