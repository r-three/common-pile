"""Install the licensed-pile package."""


import ast
import itertools
from pathlib import Path

from setuptools import find_packages, setup


def get_version(file_name: str, version_variable: str = "__version__") -> str:
    """Find the version by walking the AST to avoid duplication.

    Parameters
    ----------
    file_name : str
        The file we are parsing to get the version string from.
    version_variable : str
        The variable name that holds the version string.

    Raises
    ------
    ValueError
        If there was no assignment to version_variable in file_name.

    Returns
    -------
    version_string : str
        The version string parsed from file_name_name.
    """
    with open(file_name) as f:
        tree = ast.parse(f.read())
        # Look at all assignment nodes that happen in the ast. If the variable
        # name matches the given parameter, grab the value (which will be
        # the version string we are looking for).
        for node in ast.walk(tree):
            if isinstance(node, ast.Assign):
                if node.targets[0].id == version_variable:
                    return node.value.s
    raise ValueError(
        f"Could not find an assignment to {version_variable} " f"within '{file_name}'"
    )


with open(Path(__file__).parent / "README.md", encoding="utf-8") as f:
    LONG_DESCRIPTION = f.read()


setup(
    name="licensed_pile",
    version=get_version("licensed_pile/__init__.py"),
    description="Data Processing for the Licensed Pile",
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    author="Brian Lester",
    author_email="",
    url="https://github.com/r-three/licensed-pile",
    packages=find_packages(),
    python_requires=">=3.8",
    license="MIT",
    install_requires=["logging_json"],
)
