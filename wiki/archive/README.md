# Wiki Dumps from the Internet Archive

## Data Generation

1. Use `python get_metadata.py` to download the wiki metadata from the IA with a bit of parallelism. This creates a `ia-wiki-metadata.json` file that will be used in the rest of the scripts.
2. Use `python download_archive.py` to download and extract the actual wikis. In the future, this will also handle other wiki fetching methods like dump downloading and scraping.
3. Use `python to_dolma.py` from **this** directory to convert the IA archive wikis to the dolma format. This will save them as dolma formatted files with wikitext in the `text` field at `../data/...` by default. We need to use the `to_dolma.py` script from here as many IA wikis are in an old format that the generic dolma conversion script doesn't support.
4. Use the shared preprocessing pipeline to convert to plain text.

## Notes

We need to download 4.4 TB from the Internet Archive.

If we had a Gigabit connection it would take 9 hours to download.

Based on the Internet, people say the IA generally has a bandwidth of 10 Mbps to 1 Mbps, and the longer you download the less bandwidth they give to you.

| Bandwith | Hosts | Time to DL |
|----------|------:|-----------:|
| 1 Gb/s   | 1     | 9h 40m     |
|          | 4     | 2.3h       |
|          | 10    | 0.9h       |
| 10 Mb/s  | 1     | 40d 17h    |
|          | 4     | 10d +      |
|          | 10    | 4d +       |
| 1 Mb/s   | 1     | 407d 9h    |
|          | 4     | 101d +     |
|          | 10    | 40d+       |
|          | 100   | 4d+        |
|          | 500   | 0.8d       |

We really need hardware based parallelism
