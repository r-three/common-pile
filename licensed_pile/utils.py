"""Shared utilities like string processing."""


# We don't use snake case as the string methods added in PIP616 are named like this.
def removeprefix(s: str, prefix: str) -> str:
    """In case we aren't using python >= 3.9"""
    if s.startswith(prefix):
        return s[len(prefix) :]
    return s[:]


# We don't use snake case as the string methods added in PIP616 are named like this.
def removesuffix(s: str, suffix: str) -> str:
    """In case we aren't using python >= 3.9"""
    # Check for suffix to avoid calling s[:-0] for an empty string.
    if suffix and s.endswith(suffix):
        return s[: -len(suffix)]
    return s[:]
