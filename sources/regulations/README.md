# Regulations.gov

Proposed regulations and supporting documents published on [Regulations.gov](http://www.regulations.gov). Since each of these documents are authored by the US Federal Government, they are in the Public Domain.

# Data Collection

This collection's metadata was first gathered via a number of bulk download requests to Regulations.gov since each bulk download can only be for metadata related to a single agency over a single year span. In total, we requested metadata from 14 agencies between the years 2000 and 2023. To collect the documents, run the script `get-data.sh`. Internally this parses the metadata files to create an index of all file URLs referenced in the metadata. It then downloads all of the referenced .doc, .docx, .txt, and .htm files and converts each format to plaintext. Finally, it reads each of these converted files and stores them in a Dolma dataset. The resulting dataset is written to `data/regulations/v0`.
