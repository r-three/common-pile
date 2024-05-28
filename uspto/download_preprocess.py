from copy import copy
from typing import Literal
from unicodedata import normalize

import lxml
import lxml.html as html
import pypandoc
import requests

from licensed_pile.logs import get_logger

logger = get_logger("uspto")


def convert_mathml_to_latex(url: str, mathml_string: str) -> str:
    """Function to convert MathML to LaTeX using a REST server running https://github.com/asnunes/mathml-to-latex."""
    if not mathml_string:
        return ""
    # response = requests.post(url, json={"mathml": mathml_string})
    try:
        response = pypandoc.convert_text(mathml_string, "latex", format="html")
        return response
    except RuntimeError as e:
        logger.info(f"Error converting MathML to LaTeX: {e}")
        return mathml_string
    # if response.status_code in [400, 500]:
    #     return str(mathml_string)
    # else:
    #     result = response.json()
    #     return result.get("latex", mathml_string)


def parse_html(url, html_string: str) -> str:
    html_string = html.fromstring(html_string)
    # equations: list[html.HtmlElement] = html_string.xpath("//maths")
    # if equations:
    #     for i, eq in enumerate(equations):
    #         new_equation = convert_mathml_to_latex(url, lxml.html.tostring(eq, "unicode"))
    #         eq.clear()
    #         eq.text = new_equation
    return normalize(
        pypandoc.convert_text(
            lxml.html.tostring(html_string, encoding="unicode"), "markdown", "html"
        )
    )
