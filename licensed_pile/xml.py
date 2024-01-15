"""Tools to help with xml parsing."""

from typing import List

import lxml.etree as ET

from licensed_pile import logs


def iterate_xml(path: str, tag: str):
    """Iterable version of xml parsing, lets us not load the whole thing at once.

    Args:
      path: The path to the xml file
      tag: The tag for the xml objects we want to iterate over.

    See https://web.archive.org/web/20201111201837/http://effbot.org/zone/element-iterparse.htm
    for more details on what it is doing.
    """
    logger = logs.get_logger()
    context = ET.iterparse(path, events=("start", "end"))
    context = iter(context)
    event, root = next(context)
    try:
        for event, elem in context:
            # This `.localname` only exists for lxml. Include this or so you can
            # still do a full namespace match if you need too.
            if event == "end" and (
                ET.QName(elem.tag).localname == tag or elem.tag == tag
            ):
                yield elem
                root.clear()
    except Exception as e:
        logger.exception(f"Failed iterating over <{tag}> in {path}")


def iterate_xmls(paths: List[str], tag: str):
    """Iterable version of parsing multiple xml files with the same structure as a single iterator."""
    for path in paths:
        yield from iterate_xml(path, tag)
