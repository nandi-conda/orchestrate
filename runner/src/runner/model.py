"""Model interface and implementations.

Messages and tool schemas use the OpenAI chat-completions shape, which DeepSeek
and most other providers accept. A provider with a different wire format gets
its own class that converts to and from this shape.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Protocol

import httpx

from .secrets import get_secret


@dataclass
class ToolCall:
    id: str
    name: str
    arguments: dict[str, Any]


@dataclass
class ModelResponse:
    content: str
    tool_calls: list[ToolCall] = field(default_factory=list)
    # The assistant message to append to the conversation, as the provider wants it back.
    message: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] = field(default_factory=dict)
    reasoning: str = ""


# Called with each piece of the reply as it streams in: kind is "reasoning",
# "content" or "tool"; for "tool", name is the tool being called and text is a
# piece of its JSON arguments.
OnDelta = Callable[[str, str, str], None]


def ignore(kind: str, text: str, name: str) -> None:
    pass


class Model(Protocol):
    name: str

    async def complete(self, messages: list[dict], tools: list[dict], on_delta: OnDelta = ignore) -> ModelResponse: ...


class ModelError(RuntimeError):
    pass


class OpenAICompatModel:
    """Any OpenAI-compatible chat-completions endpoint (DeepSeek by default).

    Replies are always streamed: the thread shows reasoning, text and tool calls
    as they arrive, and a call that stops producing data fails after `stall`
    seconds. Keep-alive comments (DeepSeek sends them while a request is queued)
    do not count as data."""

    def __init__(
        self,
        model: str,
        api_key: str,
        base_url: str,
        *,
        temperature: float | None = None,
        keep_reasoning: bool = True,
        stall: float = 300.0,
        client: httpx.AsyncClient | None = None,
    ):
        self.name = model
        self.base_url = base_url.rstrip("/")
        self.temperature = temperature
        # DeepSeek thinking models want reasoning_content passed back during tool-call turns.
        self.keep_reasoning = keep_reasoning
        self.stall = stall
        self._client = client or httpx.AsyncClient(timeout=httpx.Timeout(stall, connect=30.0))
        self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    async def complete(self, messages: list[dict], tools: list[dict], on_delta: OnDelta = ignore) -> ModelResponse:
        body: dict[str, Any] = {"model": self.name, "messages": messages, "stream": True,
                                "stream_options": {"include_usage": True}}
        if tools:
            body["tools"] = tools
        if self.temperature is not None:
            body["temperature"] = self.temperature
        url = f"{self.base_url}/chat/completions"
        try:
            return await self._stream(url, body, on_delta)
        except httpx.ConnectTimeout as e:
            raise ModelError(f"{self.name}: ConnectTimeout: could not connect to {url} within 30s") from e
        except httpx.TimeoutException as e:
            raise ModelError(f"{self.name}: {type(e).__name__}: no data from POST {url} "
                             f"for {self.stall:.0f}s") from e
        except httpx.RequestError as e:
            raise ModelError(f"{self.name}: {type(e).__name__} on POST {url}: {e}") from e

    async def _stream(self, url: str, body: dict, on_delta: OnDelta) -> ModelResponse:
        content: list[str] = []
        reasoning: list[str] = []
        calls: dict[int, dict] = {}  # index -> {"id", "name", "arguments"}
        usage: dict = {}
        async with self._client.stream("POST", url, json=body, headers=self._headers) as r:
            if r.status_code != 200:
                text = (await r.aread()).decode(errors="replace")
                raise ModelError(f"{self.name}: HTTP {r.status_code}: {text[:500]}")
            last_data = time.monotonic()
            async for line in r.aiter_lines():
                if not line.startswith("data:"):  # blank separators and ": keep-alive" comments
                    if time.monotonic() - last_data > self.stall:
                        raise ModelError(f"{self.name}: only keep-alives from POST {url} for {self.stall:.0f}s")
                    continue
                last_data = time.monotonic()
                payload = line[5:].strip()
                if payload == "[DONE]":
                    break
                chunk = json.loads(payload)
                if chunk.get("error"):
                    raise ModelError(f"{self.name}: {chunk['error']}")
                if chunk.get("usage"):
                    usage = chunk["usage"]
                for choice in chunk.get("choices") or []:
                    delta = choice.get("delta") or {}
                    if delta.get("reasoning_content"):
                        reasoning.append(delta["reasoning_content"])
                        on_delta("reasoning", delta["reasoning_content"], "")
                    if delta.get("content"):
                        content.append(delta["content"])
                        on_delta("content", delta["content"], "")
                    for tc in delta.get("tool_calls") or []:
                        c = calls.setdefault(tc.get("index", 0), {"id": "", "name": "", "arguments": ""})
                        fn = tc.get("function") or {}
                        c["id"] = tc.get("id") or c["id"]
                        c["name"] += fn.get("name") or ""
                        if fn.get("arguments"):
                            c["arguments"] += fn["arguments"]
                        on_delta("tool", fn.get("arguments") or "", c["name"])
        text = "".join(content)
        out: dict[str, Any] = {"role": "assistant", "content": text}
        parsed = []
        if calls:
            out["tool_calls"] = []
            for i in sorted(calls):
                c = calls[i]
                out["tool_calls"].append({"id": c["id"], "type": "function",
                                          "function": {"name": c["name"], "arguments": c["arguments"] or "{}"}})
                try:
                    args = json.loads(c["arguments"] or "{}")
                except json.JSONDecodeError:
                    args = {"_raw": c["arguments"]}
                parsed.append(ToolCall(id=c["id"], name=c["name"], arguments=args))
        if self.keep_reasoning and reasoning:
            out["reasoning_content"] = "".join(reasoning)
        return ModelResponse(content=text, tool_calls=parsed, message=out, usage=usage,
                             reasoning="".join(reasoning))


def deepseek(model: str | None = None, api_key: str | None = None) -> OpenAICompatModel:
    key = api_key or get_secret("DEEPSEEK_API_KEY")
    if not key:
        raise ModelError("DEEPSEEK_API_KEY is not set (env or keyring)")
    return OpenAICompatModel(
        model=model or os.environ.get("DEEPSEEK_MODEL", "deepseek-v4-pro"),
        api_key=key,
        base_url=os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
    )


class ScriptedModel:
    """Replays canned responses; used by tests and offline demos."""

    name = "scripted"

    def __init__(self, responses: list[ModelResponse]):
        self.responses = list(responses)
        self.seen: list[list[dict]] = []

    async def complete(self, messages: list[dict], tools: list[dict], on_delta: OnDelta = ignore) -> ModelResponse:
        self.seen.append([dict(m) for m in messages])
        if not self.responses:
            return ModelResponse(content="(script exhausted)", message={"role": "assistant", "content": "(script exhausted)"})
        r = self.responses.pop(0)
        if r.reasoning:
            on_delta("reasoning", r.reasoning, "")
        if r.content:
            on_delta("content", r.content, "")
        for c in r.tool_calls:
            on_delta("tool", json.dumps(c.arguments), c.name)
        if not r.message:
            r.message = {"role": "assistant", "content": r.content}
            if r.tool_calls:
                r.message["tool_calls"] = [
                    {"id": c.id, "type": "function", "function": {"name": c.name, "arguments": json.dumps(c.arguments)}}
                    for c in r.tool_calls
                ]
        return r


def from_spec(spec: dict | None) -> Model:
    """Build a model from a config dict: {"provider": "deepseek", "model": "..."}."""
    spec = spec or {}
    provider = spec.get("provider", "deepseek")
    if provider == "deepseek":
        return deepseek(spec.get("model"))
    if provider == "openai-compat":
        return OpenAICompatModel(
            model=spec["model"],
            api_key=get_secret(spec.get("api_key_env", "OPENAI_API_KEY")) or "",
            base_url=spec["base_url"],
        )
    raise ModelError(f"unknown model provider: {provider}")
