from __future__ import annotations

import re
import unicodedata
import urllib.parse
from difflib import SequenceMatcher

from .errors import PaperFetchError


DOI_RE = re.compile(r"^10\.\d{4,9}/\S+$", re.IGNORECASE)
DOI_PREFIX_RE = re.compile(
    r"^(?:https?://(?:dx\.)?doi\.org/|(?:dx\.)?doi\.org/|doi:\s*)",
    re.IGNORECASE,
)
DOI_URL_RE = re.compile(
    r"^(?:https?://)?(?:dx\.)?doi\.org/",
    re.IGNORECASE,
)


def normalize_doi(value: str) -> str:
    raw = value.strip()
    if DOI_URL_RE.match(raw):
        parsed = urllib.parse.urlsplit(
            raw if raw.casefold().startswith(("http://", "https://")) else "https://" + raw
        )
        doi = urllib.parse.unquote(parsed.path.lstrip("/"))
    else:
        doi = DOI_PREFIX_RE.sub("", urllib.parse.unquote(raw), count=1)
    doi = doi.strip().lower()
    if not DOI_RE.fullmatch(doi) or any(ord(char) < 32 for char in doi):
        raise PaperFetchError(
            "validation_error",
            f"Not a valid DOI: {value!r}",
            field="doi",
        )
    return doi


def normalize_title(value: str) -> str:
    text = unicodedata.normalize("NFKC", value).casefold()
    return " ".join(re.findall(r"\w+", text, flags=re.UNICODE))


def title_similarity(left: str, right: str) -> float:
    return SequenceMatcher(None, normalize_title(left), normalize_title(right)).ratio()
