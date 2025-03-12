# Stack Exchange
##

## Data Download

**Install 7z and wget**

1. Download Sites.xml from archive.org. This is a manifest of all the sites that are available in the dump.
2. Down each dump's `.7z` file.
3. Extract each dump's `.7z` file.
Note: Stackoverflow is so large that each part of the dump (file of posts, comments, etc) are each distributed as their own `.7z`. So download those and add them to a stack overflow directory.
4. Run the `preprocess.py` script on each site dump to create dolma formatted documents. Note: Dolma sharding is applied to each site individually.

Stack Exchange posts are distributed as posts, comments, and answers that need to be joined together to create larger documents. Thus it is difficult to do the standard procedure of creating a dolma dataset of raw text and then preprocessing it with dolma. Thus we have a single `preprocess.py`  script that outputs a final dolma dataset.

Note: In addition to the questions, the comments and answers come with license information. Currently we only consider the question license.

### Community Dumps

After 2024-04, Stack Exchange has decided to stop uploading dumps to the internet archive. The community has continued to upload them (they download the dumps using stack exchanges new convoluted process and then upload to the internet archive). To use this dump, you will need to pass the url to the download index (click "view all" and then copy the url)

The new way stack exchange wants to you download dumps means that defunct "windowsphone" stackexchange dump is not accessible. If you are downloading a community dump, make sure to download and extract them from the most recent official dump. [windowsphone.stackexchange.com.7z](https://archive.org/download/stackexchange/windowsphone.stackexchange.com.7z) and [windowsphone.meta.stackexchange.com.7z](https://archive.org/download/stackexchange/windowsphone.meta.stackexchange.com.7z) as of 2025/03/11.

## Data details

| # Sites | # documents |
|--------:|------------:|
|     364 |             |

## Example

``` json
{
  "id": "6",
  "text": "Aggregate Family Rate Limits on Juniper EX\nTrying to figure out how to perform rate limits...",
  "source": "Stack Exchange",
  "added": "2024-01-12T23:11:25.048546",
  "created": "2013-05-07T20:55:35",
  "metadata": {
    "license": "Creative Commons - Attribution Share-Alike - https://creativecommons.org/licenses/by-sa/3.0/",
    "site": "networkengineering.stackexchange.com",
    "url": "https://networkengineering.stackexchange.com/questions/6",
    "authors": [
      "Craig Sirkin",
      "GoatAtWork",
      "OzNetNerd",
      "Romans Fomicevs",
      "Ron Maupin",
      "https://networkengineering.stackexchange.com/users/129",
      "https://networkengineering.stackexchange.com/users/14",
      "https://networkengineering.stackexchange.com/users/15",
      "https://networkengineering.stackexchange.com/users/16",
      "https://networkengineering.stackexchange.com/users/51",
      "https://networkengineering.stackexchange.com/users/8499",
      "user81643"
    ]
  }
}
```
