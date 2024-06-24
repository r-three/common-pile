"""Tools and utilities for parsing wikitext."""

import itertools
import re
from typing import Set

import requests

# ᙭᙭᙭ "Canadian Syllabics Chi Sign", a rare unicode that isn't touched by wtf_wikipedia
MATH_MARKER = "\u166D\u166D\u166D"
# ⇭⇭⇭ "Upwards White Arrow On Pedestal with Vertical Bar", a rare unicode untouched by wtf_wikipedia
SECOND_MARKER = "\u21ED\u21ED\u21ED"

# WTF Wikipedia strips out most templates, which is where almost all the math is :(


def replace_math_tags(text: str) -> str:
    math_opening = r'<math(?: display="?(?P<type>inline|block)"?)?>'
    math_closing = r"</math>"
    offset = 0
    new_text = []
    # Find the first math tag in the text, we will increment where we start our
    # search to be after these <math></math> tags to find the next on.
    # All regex positions are based of the text[offset:] slice we we need to add
    # offset to them when using them to index the whole string
    while math := re.search(math_opening, text[offset:]):
        # Find the closing </math> associated with this tag. This index is relative
        # to the slice of text starting with the location of the opening tag match
        # so we need to add the start offset and the offset to index into the
        # original string.
        end_start, end_end = finish_template(
            text[offset + math.span()[0] :], math_opening, math_closing
        )
        # This happens when there is a start tag but no end tag. For example,
        # in talk page 1-9564, they have `<math>` as a symbol (it is inside <nowiki>)
        if end_start == -1:
            break

        # Add everything before the first match.
        new_text.append(text[offset : offset + math.span()[0]])
        # <math display="inline"> and <math display=inline> should use $
        # <math display="block"> and <math display=block> should use $$
        # <math> always uses $$ rendering (limits above and below sum for example)
        #   but in wikipedia, if <math> is on it's own line then a new paragraph
        #   is created. In latex $$ always creates a new paragraph/line so we
        #   don't need any marking for this special case, just use $$
        new_text.append("$" if math.group("type") == "inline" else "$$")

        math_text = text[offset + math.span()[1] : offset + math.span()[0] + end_start]
        # We shouldn't have nested <math> tags, but it is wikitext so *shrug*.
        # We could recurse to replace nested <math...> tags but that would cause
        # latex errors so instead we log an error.
        # if m := re.search(math_opening, math_text):
        #     logger.error("...")
        # Add the text /after/ the opening tag, but /before/ the closing tag.
        new_text.append(math_text)
        # Same choices as above.
        new_text.append("$ " if math.group("type") == "inline" else "$$")
        # Move the search offset to /after/ the closing tag.
        offset = offset + math.span()[0] + end_end
    if text[offset:]:
        new_text.append(text[offset:])
    return "".join(new_text)


# Templates for math symbols will probably be inside other math so skip looking
# for them, list here https://en.wikipedia.org/wiki/Template:%3D
MATH_TEMPLATES = (
    "±",
    "×",
    "10^",
    "x10^",
    "abs",
    "alpha/Fe",
    "angle bracket",
    "angbr",
    "bigmath",
    "Binom",
    "bra",
    "bra-ket",
    "braket",
    "ceil",
    "closed-closed",
    "closed-open",
    "DBra",
    "Dbraket",
    "degree",
    "subst:degree",
    "Devanagari",
    "dirprod",
    "Dket",
    "e-sp",
    "ell",
    "epsilon",
    "EqNote",
    "EquationNote",
    "Equation",
    "Equation box 1",
    "EquationRef",
    "#expr:",
    "Fe/H",
    "floor",
    "gamma",
    "hub",
    "intmath",
    "intorient",
    "kappa",
    "ket",
    "lambda",
    "langle",
    "ldelim",
    "Lg-start",
    "M/H",
    "Mapsto",
    "math",
    "Math theorem",
    "Math proof",
    "math-link",
    "mathbb",
    "mathcal",
    "mexp",
    "minteg",
    "mset",
    "mu",
    "mvar",
    "mvar-link",
    "N-ary",
    "nary",  # Figure out which it actually is
    "norm",
    "Numbered block",
    "oiiint",
    "oiint",
    "open-closed",
    "open-open",
    "otimes",
    "overarc",
    "overline",
    "overset",
    "overunderset",
    "Pars",
    "phi",
    "pi",
    "pnsign",
    "radic",
    "rangle",
    "rdelim",
    "rndhands",
    "scinote",
    "sigma",
    "smallmath",
    "starred",
    "su",
    "su2",
    "sub",
    "subsub",
    "subsup",
    "sup",
    "sup sub",
    "tau",
    "theta",
    "tmath",
    "tombstone",
    "underoverset",
    "underset",
    "upsilon",
    "Urdu numeral",
    "val",
    "varepsilon",
    "varphi",
    "varsigma",
    "vartheta",
    "vec",
    "x10^",
    "xi",
    "xor",
    "φ",
    "All",
    "And",
    "Eqv",
    "Exist",
    "False",
    "Ident",
    "Imp",
    "In",
    "Models",
    "Nand",
    "Nor-",
    "Not",
    "Or-",
    "Tee",
    "True",
)

# These are templates that are hard to process and make sense to strip out.
# "change": This creates a table, skip
# "change2": This creates a table, skip
# "changes": This creates a table, skip
# "delimiter-es":
# "dice"
# "lessthan": Is used in weird substitution situations
# "underline": Seems more widespread than just math
# "var": Seems more widespread than just math
# "var serif": Seems more widespread than just math


# These are sections that are often near the end of wikipedia and have non-natural
# text after them. All lowercase for easier checks
SKIP_SECTIONS = frozenset(
    (
        "notes",
        "bibliography",
        "sources",
        "citations",
        "references",
        "see also",
        "external links",
        "further reading",
        "tertiary sources",
        "secondary sources",
        "primary sources",
        "general and cited sources",
        "footnotes",
        "works cited",
    )
)


def test_request(text, latex: bool = False):
    import requests

    r = requests.post(
        "http://localhost:5000",
        json={"wikitext": text, "source": "test", "id": "test", "latex": latex},
    )
    return r.json()


def finish_template(text, start="{{", end="}}"):
    """Find the end of a template by looking for }}.

    text should start with the {{template that we are looking to finish.
    """
    i = 0
    templates = 0
    while i < len(text):
        if m := re.match(f"^{start}", text[i:]):
            templates += 1
            # Note: .span is based on the slice so it is basically the length of the match
            i += m.span()[1] - 1
        elif m := re.match(f"^{end}", text[i:]):
            templates -= 1
            # Note: .span is based on the slice so it is basically the length of the match
            start = i + m.span()[0]
            i += m.span()[1] - 1
            if templates == 0:
                return start, i + 1
        i += 1
    return -1, -1


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


def parse_wikitext(text, doc_id, source):
    return requests.post(
        "http://localhost:5000", json={"wikitext": text, "id": doc_id, "source": source}
    ).json()["document"]


def format_section(sec) -> str:
    match sec:
        case {"title": "", "text": ""}:
            return ""
        case {"title": title, "text": ""}:
            return ""
        case {"title": "", "text": text}:
            return text
        case {"title": title, "text": text}:
            return f"{title}\n{text}"


def filter_section(sec, blocklist: Set[str] = SKIP_SECTIONS) -> bool:
    return not sec.get("title", "").lower() in blocklist


def format_document(doc, title: str = "") -> str:
    sections = filter(filter_section, doc)
    sections = (sec for s in sections if (sec := format_section(s)))
    return "\n\n".join(itertools.chain((title,), sections)).strip()


def adjust_indentation(text: str) -> str:
    """When a :indent comment is followed by a normal line, that like gets moved
    above the indentation, see https://github.com/spencermountain/wtf_wikipedia/issues/577

    This work around adds an extra newline between the list :indent line and a
    line with text to avoid this issue.

    It can cause some extra whitespace in the output, but that can easily be fixed
    later.
    """
    if indent := re.search("^:+\S+$", text, re.MULTILINE):
        # The :ident is on the last line
        if indent.span()[1] == len(text):
            return text
        if text[indent.span()[1] + 1] in (":", "\n"):
            return text[: indent.span()[1] + 1] + adjust_indentation(
                text[indent.span()[1] + 1 :]
            )
        else:
            return (
                text[: indent.span()[1] + 1]
                + "\n"
                + adjust_indentation(text[indent.span()[1] + 1 :])
            )
    return text
