import bs4
import requests


def convert_mathml_to_latex(url: str, mathml_string: str) -> dict[str, str]:
    """Function to convert MathML to LaTeX using a REST server running https://github.com/asnunes/mathml-to-latex."""
    response = requests.post(url, json={"mathml": mathml_string})
    result = response.json()
    return result


def format_text(url: str, example: dict) -> dict[str, str]:
    """
    Formats each row to:
    Title\n\n
    ABSTRACT\n\n
    abstract_text\n\n
    description
    claims

    Returned fields: text, date, app_number
    """
    output = ""
    if title := example.get("title_text"):
        output += title + "\n\n"
    if abstract := example.get("abstract_text"):
        output += (
            "ABSTRACT"
            + "\n\n"
            + bs4.BeautifulSoup(abstract, "html.parser").get_text().strip()
            + "\n\n"
        )
    if description := example.get("description_html"):
        description = bs4.BeautifulSoup(description, "html.parser")
        equations: list[bs4.element.Tag] = description.find_all("maths")
        if equations:
            for i, eq in enumerate(equations):
                new_equation = convert_mathml_to_latex(url, str(eq))["latex"]
                eq.string = new_equation
        output += description.get_text()
    if claims := example.get("claims_text"):
        claims = bs4.BeautifulSoup(claims, "html.parser")
        equations = claims.find_all("maths")
        if equations:
            for i, eq in enumerate(equations):
                new_equation = convert_mathml_to_latex(url, str(eq))["latex"]
                eq.string = new_equation
        output += claims.get_text().strip()
    return {
        "text": output,
        "date": str(example.get("publication_date")),
        "app_number": example.get("application_number"),
    }
