"""Tools and utilities for parsing wikitext."""

import itertools
import os
import re
from typing import Dict, List, Set, Tuple

import requests

# ᙭᙭᙭᙭᙭ "Canadian Syllabics Chi Sign", a rare unicode that isn't touched by wtf_wikipedia
MATH_MARKER = "\u166D\u166D\u166D\u166D\u166D"
# ⇭⇭⇭⇭⇭ "Upwards White Arrow On Pedestal with Vertical Bar", a rare unicode untouched by wtf_wikipedia
SECOND_MARKER = "\u21ED\u21ED\u21ED\u21ED\u21ED"
# ¦¦¦¦¦ "Broken Bar", `|` has a lot of meaning in wikitext so we to replace actual instances of it.
ABS_MARKER = "\u00A6\u00A6\u00A6\u00A6\u00A6"

# WTF Wikipedia strips out most templates, which is where almost all the math is
# :( What we do is find the math templates (regex to find the start then iterate
# forward to find the closing of the scope, allows for nesting) and replace them
# with a symbol that doesn't appear anywhere else. We then clean each template
# ourselves and insert them back, after wtf_wikipedia has been run on the main
# article.
#
# Sometimes wtf_wikipdia converts a template to `1/undefined` and the `/` can
# be an ascii slash or sometime various unicode verions. These are currently
# left in.


# Characters that appear in wikimath templates and how to translate them into
# how they would appear in latex.
CHAR_SYMBOLS = {
    "[Pp]hi": r"\phi",
    r"\)": ")",
    r"\(": "(",
    "[Dd]elta": r"\delta",
    "[Pp]i": r"\pi",
    "[Gg]amma": r"\gamma",
    "[Ee]psilon": r"\epsilon",
    "[Ss]igma": r"\sigma",
    "[Tt]heta": r"\theta",
    "[Vv]arepsilon": r"\epsilon",
    "[Vv]arphi": r"\phi",
    "[Vv]arsigma": r"\sigma",
    "[Vv]artheta": r"\theta",
    "[Ee]ll": r"\ell",
}


def insert_templates(text: str, templates: List[str], marker) -> str:
    """Replace each instance of marker in text with a template.

    re.sub was being annoying about \'s in the replacements.'
    """
    offset = 0
    new_text = []
    for t in templates:
        if mark := re.search(marker, text[offset:], re.IGNORECASE):
            new_text.append(text[offset : offset + mark.span()[0]])
            new_text.append(t)
            offset = offset + mark.span()[1]
        else:
            # This should be an error, but the logger isn't plumbed into this
            # function atm, just let it go for v0
            pass
    if trailing := text[offset:]:
        new_text.append(trailing)
    return "".join(new_text)


# Replacing the templates, the math tags, and the indentation adjustment all have
# basically the algorithm, but to share code the unique function part would have
# to be super complex and have access to a bunch of things (the start, end, the
# matches, etc.) So it isn't worth it to deduplicate this code at the moment.
def extract_templates(
    text: str, templates: List[str], replacement: str
) -> Tuple[str, List[str]]:
    # {{ -> { when using an f-string, this creates a regex like {{(?:(...|...)) ?\|
    # Escaping the last | is important, otherwise you match everything as the "or"
    # is an empty string.
    opening = rf"{{{{(?:{'|'.join(templates)}) *?\|"
    new_text = []
    templates = []
    offset = 0
    # See `replace_math_tags`
    while template := re.search(opening, text[offset:], re.IGNORECASE):
        # Add everything before the template
        new_text.append(text[offset : offset + template.span()[0]])
        # Find the closing }}, we expect there to be far more {{ openings inside
        # the template compared the the <math> tags, so we need to find the last
        # one. This will dispatch to the special curly parser.
        end_start, end_end = finish_template(
            text[offset + template.span()[0] :], "{{", "}}"
        )
        # If a template is opened and never finished, we just include everything
        if end_start == -1:
            offset = offset + template.span()[1]
            continue
        # Add our template replacement
        new_text.append(replacement)
        # Add the template text to our list of templates.
        templates.append(
            text[offset + template.span()[0] : offset + template.span()[0] + end_end]
        )
        # Move the search offset in the text to after the template.
        offset = offset + template.span()[0] + end_end
    # If there is any text left over after the last time we found a template,
    # add that to out new text
    if text[offset:]:
        new_text.append(text[offset:])
    # Combine the parts of the new texts.
    new_text = "".join(new_text)
    assert len(re.findall(replacement, new_text)) == len(templates)
    return new_text, templates


def remove_template_brackets(text: str, templates: List[str]) -> str:
    """This can be used to remove math templates that aren't important for latex but breaks wtf_wikipedia.

    Examples include: nobreak, nowrap, and var
    """
    for template in templates:
        opening = rf"{{{{{template} *?\|?"
        new_text = []
        offset = 0
        while t := re.search(opening, text[offset:], re.IGNORECASE):
            new_text.append(text[offset : offset + t.span()[0]])
            end_start, end_end = finish_template(
                text[offset + t.span()[0] :], "{{", "}}"
            )
            if end_start == -1:
                offset = offset + t.span()[1]
                continue
            template_text = text[
                offset + t.span()[1] : offset + t.span()[0] + end_start
            ]
            new_text.append(template_text)
            offset = offset + t.span()[0] + end_end
        if text[offset:]:
            new_text.append(text[offset:])
        text = "".join(new_text)
    return text


def fix_equals(text: str) -> str:
    """wtf_wikipedia can handle the {{math|1=...}} templates but not {{math| ... {{=}} ...}}"""
    if re.search(r"{{ ?= ?}}|<nowiki>=</nowiki>", text, re.IGNORECASE):
        text = re.sub(r"{{math ?\|", "{{math|1=", text)
        return re.sub(r"{{ ?= ?}}|<nowiki>=</nowiki>", "=", text)
    return text


##
# These function rewrite a template like {{overline|...}} to latex \overline{...}
#
def replace_template(
    text: str,
    opening,
    closing,
    start,
    end,
    nest_open=None,
    nest_close=None,
    recursive: bool = False,
) -> str:
    """Replace templates found in text with a marker. See replace_math_templates
    for an explaination of the main parsing code.

    Note: This function *always* allows for the nesting of *different* templates
          i.e., {{math|{{overline|...}}}}, but recursive=True must be set to
          allow for the nesting of the *same* template, i.e. X<sub>i<sub>j</sub></sub>
    """
    nest_open = nest_open if nest_open else opening
    nest_close = nest_close if nest_close else closing
    offset = 0
    new_text = []
    while m := re.search(opening, text[offset:], re.IGNORECASE):
        new_text.append(text[offset : offset + m.span()[0]])
        end_start, end_end = finish_template(
            text[offset + m.span()[0] :], nest_open, nest_close
        )
        if end_start == -1:
            offset = offset + m.span()[1]
            continue
        new_text.append(start)
        between = text[offset + m.span()[1] : offset + m.span()[0] + end_start]
        if recursive:
            new_text.append(
                replace_template(
                    between,
                    opening,
                    closing,
                    start,
                    end,
                    nest_open,
                    nest_close,
                    recursive,
                )
            )
        else:
            new_text.append(between)
        new_text.append(end)
        offset = offset + m.span()[0] + end_end
    if trailing := text[offset:]:
        new_text.append(trailing)
    return "".join(new_text)


##
# These are for ease of use, giving names to the common templates we replace in
# the conversion from wikitext to latex.
#
def replace_sub(text: str) -> str:
    return replace_template(text, r"<sub>", r"</sub>", "_{", "}", recursive=True)


def replace_sup(text: str) -> str:
    return replace_template(text, r"<sup>", r"</sup>", "^{", "}", recursive=True)


def replace_radical(text: str) -> str:
    opening = r"{{[Rr]adic(?:al)? ?\|"
    closing = r"}}"
    return replace_template(text, opening, closing, "\sqrt{", "}", nest_open="{{")


def replace_prime(text: str) -> str:
    opening = r"{{(?:[Pp]rime|′) ?\|"
    closing = r"}}"
    return replace_template(text, opening, closing, "", "'", nest_open="{{")


def replace_fraction(text: str) -> str:
    """{{Fraction|}} isn't handled by wtf_wikipedia but {{sfrac|...}} is."""
    text = re.sub(r"{{[Ff]ract(?:ion)?(?:/sandbox)? ?\|", "{{sfrac|", text)
    return re.sub(r"{{sfrac/sandbox ?\|", "{{sfrac|", text)


def replace_overline(text: str) -> str:
    opening = r"{{[Oo]verline ?\|?"
    closing = r"}}"
    return replace_template(text, opening, closing, r"\overline{", "}", nest_open="{{")


def replace_overbar(text: str) -> str:
    opening = r"{{[Oo]verbar ?\|"
    return replace_template(text, opening, "}}", r"\overbar{", "}", nest_open="{{")


def replace_overarc(text: str) -> str:
    opening = r"{{[Oo]verarc ?\|"
    return replace_template(text, opening, "}}", r"\overarc{", "}", nest_open="{{")


def replace_mathcal(text: str) -> str:
    opening = r"{{[Mm]athcal ?\|"
    return replace_template(text, opening, "}}", r"\mathcal{", "}", nest_open="{{")


def replace_mathbb(text: str) -> str:
    opening = r"{{[Mm]athbb ?\|"
    return replace_template(text, opening, "}}", r"\mathbb{", "}", nest_open="{{")


# TODO: Replace ''' with \mathbf{}?
def replace_strong(text: str) -> str:
    opening = r"{{[Ss]trong ?\|"
    return replace_template(text, opening, "}}", r"\mathbf{", "}", nest_open="{{")


def replace_ceil(text: str) -> str:
    opening = r"{{[Cc]eil ?\|"
    return replace_template(text, opening, "}}", r"\ceil{", "}", nest_open="{{")


def replace_floor(text: str) -> str:
    opening = r"{{[Ff]loor ?\|"
    return replace_template(text, opening, "}}", r"\floor{", "}", nest_open="{{")


def replace_norm(text: str) -> str:
    opening = r"{{[Nn]orm ?\|"
    return replace_template(
        text, opening, "}}", rf"\{ABS_MARKER}", rf"\{ABS_MARKER}", nest_open="{{"
    )


def replace_open_closed(text: str) -> str:
    opening = r"{{[Oo]pen-[Cc]losed ?\|"
    return replace_template(text, opening, "}}", "(", "]", nest_open="{{")


def replace_open_open(text: str) -> str:
    opening = r"{{[Oo]pen-[Oo]pen ?\|"
    return replace_template(text, opening, "}}", "(", ")", nest_open="{{")


def replace_closed_closed(text: str) -> str:
    opening = r"{{[Cc]losed-[Cc]losed ?\|"
    return replace_template(text, opening, "}}", "[", "]", nest_open="{{")


def replace_closed_open(text: str) -> str:
    opening = r"{{[Cc]losed-[Oo]pen ?\|"
    return replace_template(text, opening, "}}", "[", ")", nest_open="{{")


def replace_bra(text: str) -> str:
    opening = r"{{[Bb]ra ?\|"
    return replace_template(text, opening, "}}", r"\langle", ABS_MARKER, nest_open="{{")


def replace_ket(text: str) -> str:
    opening = r"{{[Kk]et ?\|"
    return replace_template(text, opening, "}}", ABS_MARKER, r"\rangle", nest_open="{{")


def replace_brace(text: str) -> str:
    opening = r"{{[Bb]race ?\|"
    return replace_template(text, opening, "}}", r"\{", r"\}", nest_open="{{")


def replace_angle_bracket(text: str) -> str:
    opening = r"{{[Aa]ngle ?[Bb]racket ?\|"
    return replace_template(text, opening, "}}", r"\langle", r"\rangle", nest_open="{{")


def replace_symbols(
    text: str, symbols: Dict[str, str] = CHAR_SYMBOLS, include_money: bool = False
) -> str:
    """Replace templates that evaulate to a symbol {{pi}} -> 𝛑 with the latex version."""
    for template, latex in symbols.items():
        # re.sub was being difficult about including something like \p in the
        # replacement string. So do it manually.
        # text = re.sub(rf"{{{{{template}}}}}", latex, text)
        if m := re.search(rf"{{{{{template}}}}}", text, re.IGNORECASE):
            if include_money:
                latex = f"${latex}$"
            text = "".join((text[: m.span()[0]], latex, text[m.span()[1] :]))
    return text


def replace_abs(text: str) -> str:
    """Convert absolute value from wikitext to latex.

    The | symbol is used in the wikitext template syntax, so they uses various
    different ways to escape them. This tries to standadize them all to the latex
    format.
    """
    text = text.replace("{{!}}", ABS_MARKER)
    text = text.replace("<nowiki>|</nowiki>", ABS_MARKER)
    text = text.replace("<nowiki>||</nowiki>", f"{ABS_MARKER}{ABS_MARKER}")
    opening = r"{{[Mm]?[Aa]bs ?\|?"
    closing = r"}}"
    return replace_template(
        text, opening, closing, ABS_MARKER, ABS_MARKER, nest_open="{{", recursive=True
    )


def replace_mset(text: str) -> str:
    """Convert set notation from wikitext to latex.

    Where are some cases where wtf_wikipedia deletes msets that have | bars in
    them despite that being legal in wikitext, those are not handled well atm.
    """
    opening = r"{{[Mm]set\|?"
    closing = r"}}"
    return replace_template(
        text, opening, closing, r"\{", r"\}", nest_open="{{", recursive=True
    )


##
# This joins together all the text processing we do.
def fix_math(text):
    """Convert wikitext math to latex.

    Note: The order of these fixes can be important, some latex output can get
          caught by regex's for other tempaltes.
    """
    text = remove_template_brackets(
        text,
        ("var", "nobreak", "nowrap", "mvar", "linktext", "em", "italics correction"),
    )
    text = fix_equals(text)
    text = replace_fraction(text)
    text = replace_prime(text)
    text = replace_overline(text)
    text = replace_overbar(text)
    text = replace_overarc(text)
    text = replace_radical(text)
    text = replace_mathcal(text)
    text = replace_mathbb(text)
    text = replace_strong(text)
    text = replace_ceil(text)
    text = replace_floor(text)
    text = replace_norm(text)
    text = replace_open_closed(text)
    text = replace_open_open(text)
    text = replace_closed_closed(text)
    text = replace_closed_open(text)
    text = replace_bra(text)
    text = replace_ket(text)
    text = replace_brace(text)
    text = replace_angle_bracket(text)
    text = replace_symbols(text)
    text = replace_sup(text)
    text = replace_sub(text)
    text = replace_mset(text)
    text = replace_abs(text)
    return text


def extract_math_templates(text: str) -> Tuple[str, List[str]]:
    """Pull all math out of the page to handle later."""
    return extract_templates(text, ("math",), MATH_MARKER)


def replace_math_tags(text: str) -> str:
    """Replace <math></math> with $$ for latex.

    We try to pick $...$ or $$...$$ based on the wikitext.
    """
    math_opening = r'<math(?: display="?(?P<type>inline|block)"?)?>'
    math_closing = r"</math>"
    offset = 0
    new_text = []
    # Find the first math tag in the text, we will increment where we start our
    # search to be after these <math></math> tags to find the next on.
    # All regex positions are based of the text[offset:] slice we we need to add
    # offset to them when using them to index the whole string
    while math := re.search(math_opening, text[offset:], re.IGNORECASE):
        # Add everything before the first match.
        new_text.append(text[offset : offset + math.span()[0]])

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
            # TODO: Add logging
            # Skip processing the scope for this one and then continue looking for more matches
            offset = offset + math.span()[1]
            continue
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
    "Function",
    "Fraction",
    "Frac",
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
    "sfrac",
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


##
# These function look ahead in the text to find the end of a scope.
def finish_template(text, start="{{", end="}}"):
    """Find the end of a template by looking for `end`.

    text should start with the `start` template that we are looking to finish.

    This handles nested scoping as long as the "start" regex matches all openings
    to scopes, otherwise it is possible to have the end of an unfound opening be
    considered the final end.
    """
    if start == "{{" and end == "}}":
        return finish_mustache_template(text)
    i = 0
    templates = 0
    while i < len(text):
        if m := re.search(f"^{start}", text[i:], re.IGNORECASE):
            templates += 1
            # Note: .span is based on the slice so it is basically the length of the match
            i += m.span()[1] - 1
        elif m := re.search(f"^{end}", text[i:], re.IGNORECASE):
            templates -= 1
            # Note: .span is based on the slice so it is basically the length of the match
            begin = i + m.span()[0]
            i += m.span()[1] - 1
            if templates == 0:
                return begin, i + 1
        i += 1
    return -1, -1


def finish_mustache_template(text):
    """This is a special case of template finding where `{` and `}` are considered
    scopes that we must close before finding }}.

    If there are } without preceding {, they are ignored.

    In ambiguous cases like {{{, it parses to {{, { for opening the scopes.
    """
    i = 2
    scopes = ["{{"]
    while i < len(text) - 1:
        if text[i] == "{":
            scopes.append("{")
        elif text[i] == "}":
            if text[i + 1] == "}":
                if scopes and scopes[-1] == "{{":
                    scopes.pop()
                    i += 1
                elif scopes and scopes[-1] == "{":
                    scopes.pop()
                if not scopes:
                    return i - 1, i + 1
            else:
                if scopes and scopes[-1] == "{":
                    scopes.pop()
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


def parse_wikitext(
    text, doc_id, source, host: str = "http://localhost", port: int = 5000
):
    """Parse wikitext by hitting a server endpoint."""
    r = requests.post(
        f"{host}:{port}",
        json={"wikitext": text, "id": doc_id, "source": source},
    )
    # This is technaially for the server to send the client when the client has
    # timed out, but there isn't a server side timeout code. 504 is for when the
    # server is a proxy, not just long running.
    if r.status_code == 408:
        raise requests.Timeout()
    # This happens when HAProxy times out
    if r.status_code == 504:
        message = r.text
        raise ValueError(f"{r}, {r.text}, probably from an HAProxy timeout.")
    if r.status_code == 200:
        try:
            return r.json()["document"]
        except requests.JSONDecodeError as e:
            e.add_note(f"JSON Decoding failed for request {r}:{r.text}")
            raise
    try:
        # Our server returns errors with json information, but if there is a non
        # 200 code because of the load balancer, it might not be as JSON.
        message = r.json()["error"]
    except requests.JSONDecodeError:
        message = r.text
    raise ValueError(message)


def format_section(sec) -> str:
    """Convert a section dict into a string like:

    title
    text...
    more text...
    ...
    """
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
    """Convert the list of sections into a string, filtering out boilerplate sections."""
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

    I had to re-write this to an iterative solution over a recursive one as the
    stack seems to be much smaller when using multiprocessing (I only the max
    recursion depth exceeded error when running within dolma).
    """
    result = []
    while indent := re.search("^:+.+$", text, re.MULTILINE | re.IGNORECASE):
        # The :ident is on the last line, "\n" isn't matched so subtract 1
        if indent.span()[1] >= (len(text) - 1):
            result.append(text)
            break

        result.append(text[: indent.span()[1] + 1])
        if text[indent.span()[1] + 1] not in (":", "\n"):
            result.append("\n")

        text = text[indent.span()[1] + 1 :]
    else:
        result.append(text)
    return "".join(result)
