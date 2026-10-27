"""HTTP client for one runner worker (see runner/API.md)."""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

import httpx

from .gitio import refs_header


class RunnerError(Exception):
    """The runner answered a request with an error status: the status, the
    request, and the runner's own words (its JSON `error`, else its body), so
    a refusal says why wherever it is logged or shown."""

    def __init__(self, status: int, method: str, path: str, error: str) -> None:
        super().__init__(f"the worker answered {status} to {method} {path}: {error}")
        self.status = status
        self.error = error


def runner_error(r: httpx.Response, body: bytes) -> RunnerError:
    try:
        error = json.loads(body).get("error") or ""
    except (ValueError, AttributeError):
        error = body.decode(errors="replace").strip()
    return RunnerError(r.status_code, r.request.method, r.request.url.path,
                       error[:2000] or f"{r.status_code} {r.reason_phrase}")


def check(r: httpx.Response) -> httpx.Response:
    """`r`, unless it is an error status: then the runner's words, as a RunnerError."""
    if r.status_code >= 400:
        raise runner_error(r, r.content)
    return r


class RunnerClient:
    def __init__(self, url: str, token: str = "", timeout: float = 30.0) -> None:
        self.url = url.rstrip("/")
        headers = {"authorization": f"Bearer {token}"} if token else {}
        self.http = httpx.AsyncClient(base_url=self.url, headers=headers, timeout=timeout)

    async def close(self) -> None:
        await self.http.aclose()

    async def health(self, timeout: float | None = None) -> dict:
        r = await self.http.get("/health", **({"timeout": timeout} if timeout else {}))
        check(r)
        return r.json()

    async def get_thread(self, thread_id: str) -> dict | None:
        """The runner's entry for the thread, None when it has none."""
        r = await self.http.get(f"/threads/{thread_id}")
        if r.status_code == 404:
            return None
        check(r)
        return r.json()

    async def create_thread(self, **body: Any) -> dict:
        r = await self.http.post("/threads", json={k: v for k, v in body.items() if v is not None})
        check(r)
        return r.json()

    async def post_message(self, thread_id: str, body: str, sender: str = "user", hops: int = 0,
                           images: list[dict] | None = None) -> None:
        payload: dict[str, Any] = {"body": body, "from": sender, "hops": hops}
        if images:
            payload["images"] = images
        r = await self.http.post(f"/threads/{thread_id}/messages", json=payload)
        check(r)

    async def stop(self, thread_id: str) -> None:
        r = await self.http.post(f"/threads/{thread_id}/stop")
        check(r)

    async def stop_if_there(self, thread_id: str) -> None:
        """Stop the thread if this runner has it; a runner that does not is fine."""
        r = await self.http.post(f"/threads/{thread_id}/stop")
        if r.status_code != 404:
            check(r)

    async def list_files(self, thread_id: str, path: str) -> dict:
        """A directory of the thread's worktree (runner/API.md, files)."""
        r = await self.http.get(f"/threads/{thread_id}/files", params={"path": path})
        return check(r).json()

    async def open_file(self, thread_id: str, path: str) -> httpx.Response:
        """A file of the thread's worktree as a streaming response; the caller
        reads it with aiter_raw and closes it."""
        req = self.http.build_request("GET", f"/threads/{thread_id}/files/raw", params={"path": path},
                                      timeout=httpx.Timeout(None, connect=10.0))
        r = await self.http.send(req, stream=True)
        if r.status_code >= 400:
            body = await r.aread()
            await r.aclose()
            raise runner_error(r, body)
        return r

    async def repo_refs(self, project: str) -> dict[str, str]:
        """The refs of the project's repository on the worker (runner/API.md, "Repository")."""
        r = await self.http.get(f"/projects/{project}/repo")
        check(r)
        return r.json()["refs"]

    async def push_repo(self, project: str, data: bytes, base: str, refs: dict[str, str]) -> None:
        """Bring the project's hub's commits to the worker: a bundle and the refs' tips."""
        r = await self.http.post(f"/projects/{project}/repo", content=data, timeout=httpx.Timeout(600.0, connect=10.0),
                                 headers={"content-type": "application/octet-stream", "x-git-base": base,
                                          "x-git-refs": refs_header(refs)})
        check(r)

    async def pull_thread(self, project: str, thread_id: str, have: list[str]) -> tuple[bytes, dict[str, str]]:
        """A thread's branch and snapshot from the worker, without the commits in `have`."""
        r = await self.http.get(f"/projects/{project}/threads/{thread_id}/repo", params={"have": ",".join(have)},
                                timeout=httpx.Timeout(600.0, connect=10.0))
        check(r)
        return r.content, json.loads(r.headers["x-git-refs"])

    async def prune(self, keep: list[str]) -> list[str]:
        """Take every project but `keep` off the worker; the ones it removed."""
        r = await self.http.post("/projects/prune", json={"keep": keep}, timeout=httpx.Timeout(120.0, connect=10.0))
        check(r)
        return r.json()["removed"]

    async def peer_calls(self, wait: float = 25.0) -> list[dict]:
        """Peer calls this worker's threads have queued (long poll; see runner/API.md)."""
        r = await self.http.get("/peer/calls", params={"wait": wait},
                                timeout=httpx.Timeout(wait + 30.0, connect=10.0))
        check(r)
        return r.json()

    async def answer_peer_call(self, call_id: str, reply: dict) -> None:
        r = await self.http.post(f"/peer/calls/{call_id}", json=reply)
        if r.status_code != 404:  # 404: the call already timed out or was answered
            check(r)

    async def events(self, after: int = 0) -> AsyncIterator[dict]:
        """Every event on this worker, from seq > after, until the stream ends."""
        async with self.http.stream("GET", "/events", params={"after": after},
                                    timeout=httpx.Timeout(None, connect=10.0)) as r:
            if r.status_code >= 400:
                raise runner_error(r, await r.aread())
            data: list[str] = []
            async for line in r.aiter_lines():
                if line.startswith("data:"):
                    data.append(line[5:].lstrip())
                elif line == "" and data:
                    yield json.loads("\n".join(data))
                    data = []
