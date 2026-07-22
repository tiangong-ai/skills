"""Reliable open-access academic paper downloader."""

from .api import fetch_paper
from .http import HttpClient, PaperTransport
from .models import FetchRequest
from .pipeline import PaperFetcher

__all__ = ["FetchRequest", "HttpClient", "PaperFetcher", "PaperTransport", "fetch_paper"]
