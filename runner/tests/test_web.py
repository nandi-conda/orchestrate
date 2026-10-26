"""DeepSeek Search & Fetch, against a mock transport."""

import asyncio

import httpx
import pytest

from runner.web import DeepSeekWeb, WebError


def web(handler) -> DeepSeekWeb:
    return DeepSeekWeb("k", "https://api.example", client=httpx.AsyncClient(transport=httpx.MockTransport(handler)))


def test_a_page_fetch_cannot_read_is_named_as_the_pages_failure():
    def handler(req):
        return httpx.Response(404, json={"error": {"message": "no extractable content"}})

    with pytest.raises(WebError) as e:
        asyncio.run(web(handler).fetch("https://example.com/gone"))
    assert str(e.value) == "web fetch https://example.com/gone: the page could not be read (no extractable content)"


def test_other_api_errors_name_the_request():
    def handler(req):
        return httpx.Response(429, json={"error": {"message": "slow down"}})

    with pytest.raises(WebError) as e:
        asyncio.run(web(handler).search("q"))
    assert "POST https://api.example/v1/tools/search: HTTP 429: slow down" in str(e.value)
