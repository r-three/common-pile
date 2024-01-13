"""Tools to help with xml parsing."""

from xml.etree import ElementTree as ET


def iterate_xml(path: str, tag: str):
    """Iterable version of xml parsing, lets us not load the whole thing at once.

    Args:
      path: The path to the xml file
      tag: The tag for the xml objects we want to iterate over.

    See https://web.archive.org/web/20201111201837/http://effbot.org/zone/element-iterparse.htm
    for more details on what it is doing.
    """
    context = ET.iterparse(path, events=("start", "end"))
    context = iter(context)
    event, root = next(context)
    for event, elem in context:
        if event == "end" and elem.tag == tag:
            yield elem
            root.clear()
