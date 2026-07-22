from __future__ import annotations

import io
import socket
import sys
import unittest
import urllib.error
import urllib.request
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))

from paper_fetch.errors import PaperFetchError
from paper_fetch.http import HttpClient
from paper_fetch.security import SafeRedirectHandler, validate_public_url


def public_dns(host: str, port: int):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", port))]


def private_dns(host: str, port: int):
    return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", port))]


class NeverOpen:
    def __init__(self):
        self.called = False

    def open(self, request, timeout):
        self.called = True
        raise AssertionError("network opener must not run")


class SecurityTests(unittest.TestCase):
    def test_private_literal_is_blocked(self):
        with self.assertRaises(PaperFetchError) as raised:
            validate_public_url("http://127.0.0.1/metadata", public_dns)
        self.assertEqual(raised.exception.code, "unsafe_url")

    def test_hostname_resolving_private_is_blocked_before_open(self):
        opener = NeverOpen()
        client = HttpClient(user_agent="test", resolver=private_dns, opener=opener)
        with self.assertRaises(PaperFetchError):
            client.get_json("https://apparently-public.example/data", timeout=1)
        self.assertFalse(opener.called)

    def test_nonstandard_port_is_blocked(self):
        with self.assertRaises(PaperFetchError):
            validate_public_url("https://example.org:8443/data", public_dns)

    def test_redirect_to_private_network_is_blocked(self):
        handler = SafeRedirectHandler(private_dns)
        request = urllib.request.Request("https://example.org/start")
        with self.assertRaises(urllib.error.HTTPError) as raised:
            handler.redirect_request(request, io.BytesIO(), 302, "Found", {}, "http://internal.example/secret")
        self.assertIn("unsafe_redirect", str(raised.exception))

    def test_public_url_passes(self):
        validate_public_url("https://example.org/paper.pdf", public_dns)


if __name__ == "__main__":
    unittest.main()
