"""HTTP client for one runner worker (see runner/API.md)."""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

import httpx

from .gitio import refs_header


class FileError(Exception):
    """The runner refused a files request (bad path, not found); its words and status."""

    def __init__(self, status: int, message: str) -> None:
        super().__init__(message)
        self.status = status


def file_error(r: httpx.Response, body: bytes) -> FileError:
    try:
        message = json.loads(body).get("error") or ""
    except ValueError:
        message = body.decode(errors="replace").strip()
    return FileError(r.status_code, message or f"HTTP {r.status_code}")


class RunnerClient:
    def __init__(self, url: str, token: str = "", timeout: float = 30.0) -> None:
        self.url = url.rstrip("/")
        headers = {"authorization": f"Bearer {token}"} if token else {}
        self.http = httpx.AsyncClient(base_url=self.url, headers=headers, timeout=timeout)

    async def close(self) -> None:
        await self.http.aclose()

    async def health(self, timeout: float | None = None) -> dict:
        r = await self.http.get("/health", **({"timeout": timeout} if timeout else {}))
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

    async def list_files(self, thread_id: str, path: str) -> dict:
        """A directory of the thread's worktree (runner/API.md, files)."""
        r = await self.http.get(f"/threads/{thread_id}/files", params={"path": path})
        if 400 <= r.status_code < 500:
            raise file_error(r, r.content)
        r.raise_for_status()
        return r.json()

    async def open_file(self, thread_id: str, path: str) -> httpx.Response:
        """A file of the thread's worktree as a streaming response; the caller
        reads it with aiter_raw and closes it."""
        req = self.http.build_request("GET", f"/threads/{thread_id}/files/raw", params={"path": path},
                                      timeout=httpx.Timeout(None, connect=10.0))
        r = await self.http.send(req, stream=True)
        if r.status_code >= 400:
            body = await r.aread()
            await r.aclose()
            if r.status_code < 500:
                raise file_error(r, body)
            r.raise_for_status()
        return r

    async def repo_refs(self) -> dict[str, str]:
        """The worker repository's refs (runner/API.md, "Repository")."""
        r = await self.http.get("/repo")
        r.raise_for_status()
        return r.json()["refs"]

    async def push_repo(self, data: bytes, base: str, refs: dict[str, str]) -> None:
        """Bring the hub's commits to the worker: a bundle and the refs' tips."""
        r = await self.http.post("/repo", content=data, timeout=httpx.Timeout(600.0, connect=10.0),
                                 headers={"content-type": "application/octet-stream", "x-git-base": base,
                                          "x-git-refs": refs_header(refs)})
        r.raise_for_status()

    async def pull_thread(self, thread_id: str, have: list[str]) -> tuple[bytes, dict[str, str]]:
        """A thread's branch and snapshot from the worker, without the commits in `have`."""
        r = await self.http.get(f"/threads/{thread_id}/repo", params={"have": ",".join(have)},
                                timeout=httpx.Timeout(600.0, connect=10.0))
        r.raise_for_status()
        return r.content, json.loads(r.headers["x-git-refs"])

    async def peer_calls(self, wait: float = 25.0) -> list[dict]:
        """Peer calls this worker's threads have queued (long poll; see runner/API.md)."""
        r = await self.http.get("/peer/calls", params={"wait": wait},
                                timeout=httpx.Timeout(wait + 30.0, connect=10.0))
        r.raise_for_status()
        return r.json()

    async def answer_peer_call(self, call_id: str, reply: dict) -> None:
        r = await self.http.post(f"/peer/calls/{call_id}", json=reply)
        if r.status_code != 404:  # 404: the call already timed out or was answered
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
