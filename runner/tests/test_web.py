import json

import httpx
import pytest

from runner import ModelResponse, ScriptedModel, ThreadHost, ToolCall
from runner import tools as T
from runner.web import DeepSeekWeb, WebError

RESULT = {"rerank_score": 2.4, "title": "DeepSeek V4 model released", "url": "https://example.com/v4",
          "snippet": "short", "summary": "DeepSeek has released the V4 model.", "content": "",
          "site_name": "Example News", "site_icon": None, "date": "2026-06-01T00:00:00+00:00", "authority": 3}


def web_with(handler) -> tuple[DeepSeekWeb, list[httpx.Request]]:
    seen: list[httpx.Request] = []

    def record(req: httpx.Request):
        seen.append(req)
        return handler(req)
    return DeepSeekWeb("k", "https://api.deepseek.com/", client=httpx.AsyncClient(transport=httpx.MockTransport(record))), seen


async def test_search_request_and_results():
    web, seen = web_with(lambda req: httpx.Response(200, json={"results": [RESULT]}))
    assert await web.search("deepseek v4", 3, "what is new in v4?") == [RESULT]
    req = seen[0]
    assert str(req.url) == "https://api.deepseek.com/v1/tools/search"
    assert req.headers["authorization"] == "Bearer k"
    assert json.loads(req.content) == {"query": "deepseek v4", "count": 3, "ori_question": "what is new in v4?"}


async def test_fetch_and_error_message():
    def handler(req: httpx.Request):
        if json.loads(req.content)["url"] == "https://ok":
            return httpx.Response(200, json={"title": "T", "content": "# T\n\nbody"})
        return httpx.Response(404, json={"error": {"message": "no usable content", "type": "x", "param": None}})
    web, seen = web_with(handler)
    assert await web.fetch("https://ok") == {"title": "T", "content": "# T\n\nbody"}
    assert str(seen[0].url) == "https://api.deepseek.com/v1/tools/fetch"
    with pytest.raises(WebError, match="HTTP 404: no usable content"):
        await web.fetch("https://gone")


async def test_thread_searches_and_fetches(tmp_path):
    def handler(req: httpx.Request):
        if req.url.path.endswith("/search"):
            return httpx.Response(200, json={"results": [RESULT]})
        return httpx.Response(200, json={"title": "V4", "content": "x" * (T.FETCH_LIMIT + 10)})
    web, _ = web_with(handler)
    model = ScriptedModel([
        ModelResponse("", [ToolCall("c1", "web_search", {"query": "deepseek v4"})]),
        ModelResponse("", [ToolCall("c2", "web_fetch", {"url": "https://example.com/v4"})]),
        ModelResponse("done", [ToolCall("c3", "finish", {"summary": "read it"})]),
    ])
    host = ThreadHost(model_factory=lambda spec: model, workdir=str(tmp_path / "t"),
                      tools=T.ALL_TOOLS + T.web_tools(web))
    t = await host.start(project="p", title="t", request="what is new in deepseek v4?")
    await host.wait_idle(t.id)
    # what the model got back, in full (events carry a shortened copy)
    search, fetch = [m["content"] for m in model.seen[-1] if m["role"] == "tool"]
    assert "1. DeepSeek V4 model released\n   https://example.com/v4" in search
    assert "Example News · 2026-06-01 · score 2.4" in search and "released the V4 model" in search
    assert fetch.startswith("# V4\n\n") and "[truncated:" in fetch
