"""Utilities for working with archived wiki dump."""

import datetime
import functools
import operator as op
import os
import re
import urllib.parse
from typing import List, Optional, Protocol, Sequence, Set, Tuple, TypeVar

import tenacity

from common_pile import licenses, logs, scrape

ENGLISH = frozenset({"en", "en-ca", "eng", "en-gb", "English", "en_hj"})
LOGGER = logs.get_logger()


def check_alive(item) -> bool:
    if "originalurl" not in item["metadata"]:
        return False
    try:
        r = scrape.get_page(item["metadata"]["originalurl"])
        return r.status_code == 200
    except tenacity.RetryError:
        return False


def check_out_of_date(item, offset: datetime):
    if "late-updated-date" not in item["metadata"]:
        return False
    last_updated = datetime.datetime.strptime(
        item["metadata"]["last-updated-date"], "%Y-%m-%d"
    )
    return False


def check_fandom(item):
    """Is a wiki a fandom wiki?"""
    url = urllib.parse.urlparse(item["metadata"]["originalurl"]).netloc
    return url.endswith("fandom.com")


def check_wikimedia(item):
    """Based on https://raw.githubusercontent.com/WikiTeam/wikiteam/master/dumpgenerator.py"""
    if m := re.findall(
        r"(?i)(wikipedia|wikisource|wiktionary|wikibooks|wikiversity|wikimedia|wikispecies|wikiquote|wikinews|wikidata|wikivoyage)\.org",
        item["metadata"]["originalurl"],
    ):
        return True
    return False


# TODO: Some wiki's licenses don't match the metadata in the uploaded dump. We
# should use the AI2 CC license tool to verify that the metadata and license on
# the actual website match.
def verify_license(item):
    return True


def filter_language(lang: Optional[str], allowed: Set[str] = ENGLISH) -> bool:
    if lang:
        return lang in allowed or lang == "Unknown"
    # If no language (is None), try it because a lot of the internet is English.
    return True


def find_date(s: str) -> Optional[datetime.datetime]:
    """Lots of identifiers or filenames have dates in them, IA generally uses YYYYMMDD."""
    for regex in [
        r"-(?P<date>\d{8})-",
        r"(?P<date>\d{8})",
        r"-(?P<date>\d{4}-\d{2}-\d{2})",
    ]:
        if date_str := re.search(regex, s):
            try:
                date_str = date_str.group("date").replace("-", "")
                return datetime.datetime.strptime(date_str, "%Y%m%d")
            except Exception as e:
                LOGGER.warning(f"Failed to parse {date_str} into a real date.")
    LOGGER.warning(f"Failed to find date string in {s}")


def parse_version(s: str) -> Tuple[int, int, int]:
    """Based on https://semver.org/#is-there-a-suggested-regular-expression-regex-to-check-a-semver-string"""
    if v := re.search(
        r"(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)(?:\.(?P<patch>0|[1-9]\d*))?(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?",
        s,
    ):
        # If there isn't a patch version, assume it is .0
        return (
            int(v.group("major")),
            int(v.group("minor")),
            int(v.group("patch")) if v.group("patch") is not None else 0,
        )
    LOGGER.warning(f"Failed to parse version from {s}")
    return (0, 0, 0)


# This was a class used to tell which version of scraping tools was used to
# collect this wiki. It used to be a sign of the format used for uploading
# however, that pattern no longer holds and we need to infer that from the
# files themselves. This is not currently used.
class Scanner(licenses.StringEnum):
    WIKITEAM_3 = "wikiteam3"
    IA_PYTHON = "Internet Archive Python Library >= 1.0.0"
    IA_PYTHON_OLD = "Internet Archive Python Library < 1.0.0"
    IA_HTML5 = "Internet Archive HTML5 Uploader"
    UNKNOWN = "unknown"

    @classmethod
    def from_string(cls, s: str) -> "Scanner":
        if s is None:
            return cls.UNKNOWN
        if s.startswith("wikiteam3"):
            return cls.WIKITEAM_3
        if s.startswith("Internet Archive HTML5 Uploader"):
            return cls.IA_HTML5
        if s.startswith("Internet Archive Python library"):
            version = parse_version(s)
            if version < (1, 0, 0):
                return cls.IA_PYTHON_OLD
            return cls.IA_PYTHON
        return cls.UNKNOWN


M = TypeVar("M")


# Each IA metadata blob comes with a list of files available to download.
# These can be different formats depending on how/when they were uploaded.
# There can also be files that we don't care about, e.g., images
# These finds find the files that we are most likely to want to download.
class FileFinder(Protocol):
    def __call__(self, file_metadata: Sequence[M], ident: str) -> List[M]:
        """Check if any of the files are worth downloading."""


def find_history(file_metadata, ident):
    # Remove the - instead as some files are just called history.xml.gz
    LOGGER.debug("Searching for history.xml files to download.")
    return [f for f in file_metadata if "history.xml" in f["name"]]
    # LOGGER.debug("Searching for -history.xml files to download.")
    # return [f for f in file_metadata if "-history.xml" in f["name"]]


def find_compressed(file_metadata, ident, ext: str):
    LOGGER.debug(f"Searching for {ident}.{ext} files to download")
    return [f for f in file_metadata if f["name"] == f"{ident}.{ext}"]


find_zip = functools.partial(find_compressed, ext="zip")
find_7z = functools.partial(find_compressed, ext="7z")
find_gz = functools.partial(find_compressed, ext="gz")


def find_identifier(file_metadata, ident):
    LOGGER.debug(
        f"Searching for files to download with the same name as the identifier: {ident}"
    )
    return [f for f in file_metadata if f["name"] == ident]


def find_wikidump(file_metadata, ident):
    LOGGER.debug(f"Searching for files that end in -wikidump to download.")
    return [
        f for f in file_metadata if os.path.splitext(f["name"])[0].endswith("-wikidump")
    ]


def find_history_zipped(file_metadata, ident):
    LOGGER.debug(f"Searching for compressed files that end in -history to download.")
    return [
        f for f in file_metadata if os.path.splitext(f["name"])[0].endswith("-history")
    ]


def find_pages_full(file_metadata, ident):
    LOGGER.debug(f"Searching for files that end in _pages_full.xml")
    return [
        f
        for f in file_metadata
        if os.path.splitext(f["name"])[0].endswith("_pages_full.xml")
    ]


def find_pages(file_metadata, ident):
    LOGGER.debug(f"Search for pages.xml files to download.")
    return [f for f in file_metadata if "pages.xml" in f["name"]]


def find_ident_plus_date(file_metadata, ident):
    LOGGER.debug(f"Search for file formatted as ident-?\d+?\..*?")
    return [
        f
        for f in file_metadata
        if re.match(rf"{ident}-?\d+?\..*?", f["name"], re.IGNORECASE)
    ]


def find_complete(file_metadata, ident):
    LOGGER.debug(f"Searching for files that end in -complete")
    return [
        f
        for f in file_metadata
        if re.search(r"-complete(7z)?$", os.path.splitext(f["name"])[0])
    ]


def find_xmlonly(file_metadata, ident):
    LOGGER.debug(f"Searching for files that end in -wikidump.XMLONLY")
    return [
        f
        for f in file_metadata
        if os.path.splitext(f["name"])[0].endswith("-wikidump.XMLONLY")
    ]


def find_wikidumper(file_metadata, ident):
    LOGGER.debug(f"Searching for files that end with dumped_using_wikidumper")
    return [f for f in file_metadata if "dumped_using_wikidumper" in f["name"]]


def find_gzipped_xml(file_metadata, ident):
    LOGGER.debug(f"Searching for files that end with .xml.gz")
    return [f for f in file_metadata if f["name"].endswith(".xml.gz")]


def find_current(file_metadata, ident):
    LOGGER.debug(f"Searching for files with -current")
    return [f for f in file_metadata if "-current" in f["name"]]


def find_download(
    item_metadata,
    file_finders: Sequence[FileFinder] = (
        find_history,
        find_zip,
        find_7z,
        find_gz,
        find_wikidump,
        find_history_zipped,
        find_pages_full,
        find_pages,
        find_identifier,
        find_ident_plus_date,
        find_complete,
        find_xmlonly,
        find_wikidumper,
        find_gzipped_xml,
        find_current,
    ),
):
    """Given the wiki metadata, find which file of the available is the one we should download."""
    ident = re.sub(r"^[wW]iki-", "", item_metadata["metadata"]["identifier"])
    for file_fn in file_finders:
        dl_files = file_fn(item_metadata["files"], ident)
        if dl_files:
            break
    # This else runs if we didn't break out of the loop, that it, we didn't find
    # good files to download.
    else:
        LOGGER.error(f"Failed to find files to download for {ident}")
        return None

    # If multiple uploads have happened find the most recent one.
    if len(dl_files) > 1:
        dates = [find_date(f["name"]) for f in dl_files]
        # argmax
        _, dl_file = max(zip(dates, dl_files), key=op.itemgetter(0))
    else:
        dl_file = dl_files[0]
    return dl_file


# TODO: This function is duplicated, unify and remove. Need better way to
# share the code between the different subdirs (dump/, archive/, scrape/)
def wiki_to_dir(wiki_id, chars: int = 2, levels: int = 2):
    """Convert wiki id to a nested dir for faster filesystem access.

    ex: wiki-car_collectionfandomcom -> wiki-ca/r_/wiki-car_collectionfandomcom
    """
    prefix = "wiki-" if wiki_id.startswith("wiki-") else ""
    wiki_id = re.sub(f"^{prefix}", "", wiki_id)
    parts = (
        (f"{prefix}{wiki_id[:chars]}",)
        + tuple(wiki_id[l * chars : (l + 1) * chars] for l in range(1, levels))
        + (f"{prefix}{wiki_id}",)
    )
    return os.path.join(*parts)


def zst_uncompress(compressed, uncompressed=None):
    """Uncompress a file using zstd with the `--long=31` flag."""
    if uncompressed is None:
        uncompressed = re.sub(r"\.zst$", "", compressed)
        LOGGER.info(
            "Uncompressed target for {compressed} not provided, using {uncompressed}"
        )
    with open(uncompressed, "w") as wf:
        # TODO: Add error handling, namely, delete the (wrong) uncompressed
        # file is there are errors.
        subprocess.run(
            ["/usr/bin/zstd", "-c", "-d", "--long=31", "--", compressed], stdout=wf
        )
    return uncompressed


KNOWN_BAD = frozenset(
    {
        # Only has a torrent file
        "hunty_cities_skylinesfandomcom",
        "kenyonushistory.wikispaces.com",
        "Wiki-Puella-magi.net",
        "Wiki-Tanasinn.info",
        "Wiki-Velowiki.org",
        "fanhistory.com",
        "hampediaorg-20111022-dump",
        "wiki-biblioteca_wikimedia_it",
        "wiki-cencilia_fashion_fantasyfandomcom",
        "wiki-cynthiaokolo.wikispaces.com",
        "wiki-edgetechnology.wikispaces.com",
        "wiki-elictmentor.wikispaces.com",
        "wiki-ems-teacher.wikispaces.com",
        "wiki-es-tech.wikispaces.com",
        "wiki-guild_of_heroes_2fandomcom",
        "wiki-hchsapes.wikispaces.com",
        "wiki-hostagesfandomcom",
        "wiki-hunty_cities_skylinesfandomcom",
        "wiki-icomputers3.wikispaces.com",
        "wiki-kellys-oabcig-pe.wikispaces.com",
        "wiki-kenyonushistory.wikispaces.com",
        "wiki-misssherk.wikispaces.com",
        "wiki-mrsburns.wikispaces.com",
        "wiki-mixelsstories.fandom.com-20230912",
        "wiki-maou-gakuen-no-hangyakusha-03282023",
        "wiki-boyinstripedpyjamas.wikispaces.com_meta",
        "wiki-httpsverifiedhandles.comvhidmain_page",
        "tropicalwikis-feb-2013",
        "Wiki-PCGamingWiki_201310",
        "wiki-stockerlibrary.wikispaces.com",
        "wiki-rapidpopulationgrowth.wikispaces.com",
        "wiki-rgfigreenschool.wikispaces.com",
        "wiki-robots2011.wikispaces.com",
        "wiki-rtdufodefensefandomcom",
        "wiki-scc2011.wikispaces.com",
        "wiki-spanish1h.wikispaces.com",
        "wiki-starter-old.fandom.com_ar-20230912",
        "wiki-starter-old.fandom.com_he-20230912",
        "wiki-processors.wiki.ti.com_20140107",
        "wiki-webquests215.wikispaces.com",
        "wiki-wiki.aynu.org",
        "wikimediacommons-torrents",
        "wikipediapresentations",
        "wikipediappt",
        "wiki-www.alphalinux.org",
        # This is a dump of multiple wikis, These will be handled specially.
        "shoutwiki.com",
        # These are dumps of MediaWiki things so we should get them from official
        # sources.
        "WikitravelAllLanguages.7z",
        "wikia_dump_20200214",
        "wikivoyage",
        # These are dumps of wikipedia pages that were quickly deleted.
        # They should be handled specially
        "wikipedia-delete-v3-2012-07",
        # These are older versions of the speedy deletion dumps
        "wikipedia-delete-v2-2012-06",
        "wikipedia-delete-2012-06",
        "wikipedia-delete-2012-05",
        # Non-standard format/filename and not in english, not worth fixing
        "wiki-chiliwikide",
        "wiki-de-media-perdida",
        "wiki-biblioteca_wikimedia_it_20140110",
        # This is an older version of religionswiki-20200920-wikidump (which is in a
        # weird formet :/)
        "religionswiki-20190926-wikidump.tar",
        # These are older versions of chuunibyou-demo-koi-ga-shitai-fandom_20210201
        "wiki-chuunibyou-demo-koi-ga-shitai-fandom_202008",
        "wiki-chuunibyou-demo-koi-ga-shitai-fandom_202009",
        "wiki-chuunibyou-demo-koi-ga-shitai-fandom_202102",
        # This dump is formatted really wrong
        "wiki-chuunibyou-demo-koi-ga-shitai-fandom_20210201",
        # This dump is a weird epub format
        "wiki-nsindexnet",
        # The dump files have strange names (it is co instead of to, etc.) and the
        # content is in Russian
        "wiki-lurkmoreto",
        # Non English and the dump mentions that the the License doesn't apply to all
        # the content
        "wiki-es.wikieducator.org_201401",
        "wiki-fr.wikieducator.org_201401",
        # This dump seems to only include the images
        "wiki-battleborn.fandom.com-20240323",
        # These are older versions of the multi-wiki dump wikia_dump_20200214
        "wikia_dump_20121109",
        "wikia_dump_20121204",
        "wikia_dump_20140125",
        "wikia_dump_20140529",
        "wikia_dump_20141219",
        "wikia_dump_20180602",
        # These are older versions of the dump wikiironchariotsorg-20170712-wikidump.tar
        "wikiironchariotsorg-20150805-wikidump.tar",
        "wikiironchariotsorg-20160714",
        # This is a just a log of what wiki's they have dumpped.
        "wikiteam-2018-05",
        "wikiteam_2020-02-09",
        # This is an old version of wiki-windowswallpapermirahezeorg_w
        "wiki-windowswallpapermirahezeorg-20220509",
        # This was causing errors when downloading.
        "wiki-galaxiesunboundfandomcom",
        "wiki-doomwiki.org-20231010",
    }
)
