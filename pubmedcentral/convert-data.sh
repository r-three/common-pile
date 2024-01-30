#!/usr/bin/env sh

# combine file lists
# 'FNR>1' skips the first line of each file
awk 'FNR>1' data/oa_comm_xml.PMC0*xxxxxx.baseline.2023-12-18.permissive_filelist.txt > data/oa_comm_xml.permissive_filelist.txt

# remove newline from end of file
truncate -s-1 data/oa_comm_xml.permissive_filelist.txt

# remove old file lists
rm data/oa_comm_xml.PMC0*xxxxxx.baseline.2023-12-18.permissive_filelist.txt

# convert files to markdown
python3 convert_to_md.py --filelist data/oa_comm_xml.permissive_filelist.txt
