"""Model interface and implementations.

Messages and tool schemas use the OpenAI chat-completions shape, which DeepSeek
and most other providers accept. A provider with a different wire format gets
its own class that converts to and from this shape.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Protocol

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


class Model(Protocol):
    name: str

    async def complete(self, messages: list[dict], tools: list[dict]) -> ModelResponse: ...


class ModelError(RuntimeError):
    pass


class OpenAICompatModel:
    """Any OpenAI-compatible chat-completions endpoint (DeepSeek by default)."""

    def __init__(
        self,
        model: str,
        api_key: str,
        base_url: str,
        *,
        temperature: float | None = None,
        keep_reasoning: bool = True,
        timeout: float = 300.0,
        client: httpx.AsyncClient | None = None,
    ):
        self.name = model
        self.base_url = base_url.rstrip("/")
        self.temperature = temperature
        # DeepSeek thinking models want reasoning_content passed back during tool-call turns.
        self.keep_reasoning = keep_reasoning
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    async def complete(self, messages: list[dict], tools: list[dict]) -> ModelResponse:
        body: dict[str, Any] = {"model": self.name, "messages": messages}
        if tools:
            body["tools"] = tools
        if self.temperature is not None:
            body["temperature"] = self.temperature
        r = await self._client.post(f"{self.base_url}/chat/completions", json=body, headers=self._headers)
        if r.status_code != 200:
            raise ModelError(f"{self.name}: HTTP {r.status_code}: {r.text[:500]}")
        data = r.json()
        msg = data["choices"][0]["message"]
        calls = []
        for tc in msg.get("tool_calls") or []:
            raw = tc["function"].get("arguments") or "{}"
            try:
                args = json.loads(raw)
            except json.JSONDecodeError:
                args = {"_raw": raw}
            calls.append(ToolCall(id=tc["id"], name=tc["function"]["name"], arguments=args))
        out = {"role": "assistant", "content": msg.get("content") or ""}
        if msg.get("tool_calls"):
            out["tool_calls"] = msg["tool_calls"]
        if self.keep_reasoning and msg.get("reasoning_content"):
            out["reasoning_content"] = msg["reasoning_content"]
        return ModelResponse(content=msg.get("content") or "", tool_calls=calls, message=out, usage=data.get("usage", {}))


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

    async def complete(self, messages: list[dict], tools: list[dict]) -> ModelResponse:
        self.seen.append([dict(m) for m in messages])
        if not self.responses:
            return ModelResponse(content="(script exhausted)", message={"role": "assistant", "content": "(script exhausted)"})
        r = self.responses.pop(0)
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
