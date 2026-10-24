"""HTTP client for one runner worker (see runner/API.md)."""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

import httpx


class RunnerClient:
    def __init__(self, url: str, token: str = "", timeout: float = 30.0) -> None:
        self.url = url.rstrip("/")
        headers = {"authorization": f"Bearer {token}"} if token else {}
        self.http = httpx.AsyncClient(base_url=self.url, headers=headers, timeout=timeout)

    async def close(self) -> None:
        await self.http.aclose()

    async def health(self) -> dict:
        r = await self.http.get("/health")
        r.raise_for_status()
        return r.json()

    async def create_thread(self, **body: Any) -> dict:
        r = await self.http.post("/threads", json={k: v for k, v in body.items() if v is not None})
        r.raise_for_status()
        return r.json()

    async def post_message(self, thread_id: str, body: str, sender: str = "user", hops: int = 0) -> None:
        r = await self.http.post(f"/threads/{thread_id}/messages",
                                 json={"body": body, "from": sender, "hops": hops})
        r.raise_for_status()

    async def stop(self, thread_id: str) -> None:
        r = await self.http.post(f"/threads/{thread_id}/stop")
        r.raise_for_status()

    async def events(self, after: int = 0) -> AsyncIterator[dict]:
        """Every event on this worker, from seq > after, until the stream ends."""
        async with self.http.stream("GET", "/events", params={"after": after},
                                    timeout=httpx.Timeout(None, connect=10.0)) as r:
            r.raise_for_status()
            data: list[str] = []
            async for line in r.aiter_lines():
                if line.startswith("data:"):
                    data.append(line[5:].lstrip())
                elif line == "" and data:
                    yield json.loads("\n".join(data))
                    data = []
