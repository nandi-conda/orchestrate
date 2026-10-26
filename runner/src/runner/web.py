"""Web search and page fetch through DeepSeek's Search & Fetch API.

Both endpoints take the same key as the chat model (Authorization: Bearer
$DEEPSEEK_API_KEY) and answer plain JSON:

    POST {base}/v1/tools/search  {"query", "count", "ori_question"} -> {"results": [...]}
    POST {base}/v1/tools/fetch   {"url"}                            -> {"title", "content"}

Errors come back in the OpenAI shape, {"error": {"message", ...}}. Fetch
answers 404 when the page itself could not be read (gone, blocked, an error
from the site, no extractable content), so that is the page's failure, not
the API's. See https://api-docs.deepseek.com/guides/search.
"""

from __future__ import annotations

import logging
import os

import httpx

from .secrets import get_secret

log = logging.getLogger("runner.web")


class WebError(RuntimeError):
    pass


class DeepSeekWeb:
    def __init__(self, api_key: str, base_url: str, *, client: httpx.AsyncClient | None = None):
        self.base_url = base_url.rstrip("/")
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(60.0, connect=30.0))
        self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    async def _post(self, path: str, body: dict, what: str) -> dict:
        """POST to the API; WebError says what failed in `what`'s words."""
        url = f"{self.base_url}{path}"
        r = await self._client.post(url, json=body, headers=self._headers)
        if r.status_code != 200:
            try:
                message = r.json()["error"]["message"]
            except Exception:
                message = r.text[:500]
            log.info("%s: HTTP %s: %s", what, r.status_code, message)
            if path == "/v1/tools/fetch" and r.status_code == 404:
                raise WebError(f"{what}: the page could not be read ({message})")
            raise WebError(f"{what}: POST {url}: HTTP {r.status_code}: {message}")
        log.info("%s: ok", what)
        return r.json()

    async def search(self, query: str, count: int = 10, question: str | None = None) -> list[dict]:
        body: dict = {"query": query, "count": count}
        if question:
            body["ori_question"] = question
        return (await self._post("/v1/tools/search", body, f"web search {query!r}"))["results"]

    async def fetch(self, url: str) -> dict:
        return await self._post("/v1/tools/fetch", {"url": url}, f"web fetch {url}")


def deepseek_web(api_key: str | None = None) -> DeepSeekWeb:
    key = api_key or get_secret("DEEPSEEK_API_KEY")
    if not key:
        raise WebError("DEEPSEEK_API_KEY is not set (env or keyring)")
    return DeepSeekWeb(key, os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com"))
