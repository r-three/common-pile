# Ubuntu IRC
##

Logs of IRC chat discussions around the Ubuntu Linux Distro

"The content of all Ubuntu channels, whether official logs or otherwise, are considered to be in the public domain."

## Data Download

The `get-chats.sh` script can be used to download the chat logs via `wget`.

The `to-dolma.py` script is used to convert the data from the downloaded structure (directory tree encoding the date) into the dolma format.

The `preprocess.py` script is used to clean up the chats and extract author information.

Cleaning steps done:
* Channel announcements (for example users changing names, etc) are removed.
* Telegram bot names are removed
* Bot chats are removed

Each document is the log of a single chat room in a given day. The document id is structred as `yyyy-mm-dd-#{channel_name}`. Some documents may be empty or very short. Author information is extracted an saved as metadata, but the author information/chat structure (timestamps and the like) remain in the text.

## Data Stats

| # Chats |
|--------:|
|  700715 |

Start Date: 2004-07-05
End Date:

## Example

``` json
{
  "id": "2013-04-29-#ubuntu-app-devel",
  "text": "[10:19] <Marlinc> How do I use the sync menu from Python?\n[21:57] <matzipan> anyone around?...",
  "source": "ubuntu-chat",
  "added": "2024-01-12T19:40:10.630330",
  "created": "2013-04-29",
  "metadata": {
    "license": "Public Domain",
    "authors": [
      "Marlinc",
      "cor3ntin",
      "matzipan",
      "stqn"
    ],
    "url": "https://irclogs.ubuntu.com/2013/04/29/%23ubuntu-app-devel.txt",
    "channel": "#ubuntu-app-devel"
  }
}
```
