"""Name and text cleaning helpers used by the matching pipeline.

The Databricks notebooks currently keep the Spark expressions inline so they can
run as standalone notebooks. These Python helpers document the same logic and
can be reused later if the notebooks are converted into a package-driven job.
"""

import re

LEGAL_SUFFIX_PATTERN = re.compile(
    r"\b(inc|incorporated|ltd|limited|llc|plc|corp|corporation|co|company|ag|sa|bv|nv|gmbh|pte|pty|holdings|holding|group)\b",
    flags=re.IGNORECASE,
)


def clean_name(value: str | None) -> str:
    """Return a normalized company name for approximate matching."""
    if value is None:
        return ""

    text = value.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = LEGAL_SUFFIX_PATTERN.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def first_token(value: str | None) -> str:
    """Return the first token from a cleaned name."""
    cleaned = clean_name(value)
    return cleaned.split()[0] if cleaned else ""


def name_prefix(value: str | None, length: int = 4) -> str:
    """Return a short prefix from a cleaned name for simple blocking."""
    return clean_name(value)[:length]
