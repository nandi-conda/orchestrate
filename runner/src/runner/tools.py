"""Tools a thread can call. Each tool is a JSON schema plus an async handler."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Awaitable, Callable

if TYPE_CHECKING:
    from .thread import AgentThread
    from .web import DeepSeekWeb

Handler = Callable[["AgentThread", dict], Awaitable[str]]


@dataclass
class Tool:
    name: str
    description: str
    parameters: dict
    handler: Handler

    def schema(self) -> dict:
        return {"type": "function", "function": {"name": self.name, "description": self.description, "parameters": self.parameters}}


def _obj(props: dict, required: list[str]) -> dict:
    return {"type": "object", "properties": props, "required": required}


S = {"type": "string"}


async def _bash(t: "AgentThread", a: dict) -> str:
    r = await t.sandbox.exec(a["command"], timeout=float(a.get("timeout", 120)))
    parts = [f"exit {r.exit_code}" + (" (timed out)" if r.timed_out else "")]
    if r.stdout:
        parts.append(f"stdout:\n{r.stdout}")
    if r.stderr:
        parts.append(f"stderr:\n{r.stderr}")
    return "\n".join(parts)


async def _read(t: "AgentThread", a: dict) -> str:
    return (await t.sandbox.read_file(a["path"])).decode("utf-8", errors="replace")


async def _write(t: "AgentThread", a: dict) -> str:
    await t.sandbox.write_file(a["path"], a["content"].encode())
    t.files.add(a["path"])
    return f"wrote {len(a['content'])} chars to {a['path']}"


async def _edit(t: "AgentThread", a: dict) -> str:
    text = (await t.sandbox.read_file(a["path"])).decode()
    n = text.count(a["old"])
    if n == 0:
        return "error: old text not found"
    if n > 1 and not a.get("all"):
        return f"error: old text found {n} times; make it unique or pass all=true"
    text = text.replace(a["old"], a["new"]) if a.get("all") else text.replace(a["old"], a["new"], 1)
    await t.sandbox.write_file(a["path"], text.encode())
    t.files.add(a["path"])
    return f"edited {a['path']} ({n if a.get('all') else 1} replacement)"


async def _ls(t: "AgentThread", a: dict) -> str:
    return "\n".join(await t.sandbox.list_dir(a.get("path", ".")))


async def _finish(t: "AgentThread", a: dict) -> str:
    t.finish_requested = True
    if a.get("summary"):
        t.summary = a["summary"]
    return "ok"


async def _list_threads(t: "AgentThread", a: dict) -> str:
    rows = await t.peers.list_threads(t.project, a.get("query"))
    return json.dumps([r for r in rows if r.get("id") != t.id], indent=1)


async def _read_thread(t: "AgentThread", a: dict) -> str:
    return json.dumps(await t.peers.read_thread(a["id"], int(a.get("last_n", 20))), indent=1)[:20_000]


async def _send(t: "AgentThread", a: dict) -> str:
    return json.dumps(await t.peers.send_message(t.id, a["to"], a["body"], a.get("reply_to")))


async def _spawn(t: "AgentThread", a: dict) -> str:
    if t.spawned >= t.limits.max_spawn:
        return f"error: spawn limit ({t.limits.max_spawn}) reached for this thread"
    t.spawned += 1
    return json.dumps(await t.peers.spawn_thread(t.id, t.project, a["title"], a["request"]))


async def _summary(t: "AgentThread", a: dict) -> str:
    t.summary = a["summary"]
    await t.peers.set_summary(t.id, t.summary, sorted(t.files))
    return "ok"


WORK_TOOLS = [
    Tool("bash", "Run a shell command in this thread's workspace (a git worktree of the project).",
         _obj({"command": S, "timeout": {"type": "number", "description": "seconds, default 120"}}, ["command"]), _bash),
    Tool("read_file", "Read a file, path relative to the workspace.", _obj({"path": S}, ["path"]), _read),
    Tool("write_file", "Create or overwrite a file.", _obj({"path": S, "content": S}, ["path", "content"]), _write),
    Tool("edit_file", "Replace exact text in a file. `old` must be unique unless all=true.",
         _obj({"path": S, "old": S, "new": S, "all": {"type": "boolean"}}, ["path", "old", "new"]), _edit),
    Tool("list_dir", "List a directory.", _obj({"path": S}, []), _ls),
    Tool("finish", "End this turn and land your branch on the project's main branch. Give a one-paragraph "
         "summary of what you did and the state you left things in.",
         _obj({"summary": S}, ["summary"]), _finish),
]

PEER_TOOLS = [
    Tool("list_threads", "List the other threads in this project: id, title, state, summary, files touched.",
         _obj({"query": S}, []), _list_threads),
    Tool("read_thread", "Read another thread's summary and recent events.",
         _obj({"id": S, "last_n": {"type": "integer"}}, ["id"]), _read_thread),
    Tool("send_message", "Send a message to another thread's inbox. Use it to hand off work, ask a question, or warn about a conflict.",
         _obj({"to": S, "body": S, "reply_to": S}, ["to", "body"]), _send),
    Tool("spawn_thread", "Start a new thread for an independent piece of work. Prefer spawning over doing unrelated work yourself. Its result comes back to you as a message.",
         _obj({"title": S, "request": S}, ["title", "request"]), _spawn),
    Tool("set_summary", "Update your one-paragraph status in the thread directory so others and the router know what you own.",
         _obj({"summary": S}, ["summary"]), _summary),
]

ALL_TOOLS = WORK_TOOLS + PEER_TOOLS

FETCH_LIMIT = 50_000  # characters of a fetched page handed to the model


def _result(i: int, r: dict) -> str:
    meta = [m for m in (r.get("site_name"), (r.get("date") or "")[:10], f"score {r.get('rerank_score')}") if m]
    return f"{i}. {r['title']}\n   {r['url']}\n   {' · '.join(meta)}\n   {r.get('summary') or r.get('snippet', '')}"


def web_tools(web: "DeepSeekWeb") -> list[Tool]:
    """web_search and web_fetch, answered by `web`."""

    async def _search(t: "AgentThread", a: dict) -> str:
        results = await web.search(a["query"], int(a.get("count", 10)), a.get("question"))
        if not results:
            return "no results"
        return "\n\n".join(_result(i, r) for i, r in enumerate(results, 1))

    async def _fetch(t: "AgentThread", a: dict) -> str:
        page = await web.fetch(a["url"])
        content = page["content"]
        if len(content) > FETCH_LIMIT:
            content = content[:FETCH_LIMIT] + f"\n\n[truncated: {len(page['content'])} chars, showing the first {FETCH_LIMIT}]"
        return f"# {page['title']}\n\n{content}" if page.get("title") else content

    return [
        Tool("web_search", "Search the web. Returns ranked results with title, URL, date and a summary. The query is "
             "at most 128 characters and supports operators: site:, -site:, \"exact phrase\", -term, lang:en, "
             "after:2025-01-01, before:2026. Use web_fetch to read a result in full.",
             _obj({"query": S, "count": {"type": "integer", "description": "results, 1 to 50, default 10"},
                   "question": {"type": "string", "description": "the question behind the search (at most 128 "
                                "characters); it helps the engine understand the query"}}, ["query"]), _search),
        Tool("web_fetch", "Fetch a web page (http or https URL) and return its content as Markdown.",
             _obj({"url": S}, ["url"]), _fetch),
    ]


async def call(tools: dict[str, Tool], t: "AgentThread", name: str, args: dict) -> str:
    tool = tools.get(name)
    if not tool:
        return f"error: unknown tool {name}"
    try:
        return await tool.handler(t, args)
    except KeyError as e:
        return f"error: missing argument {e}"
    except Exception as e:  # tool errors go back to the model, not up the stack
        return f"error: {type(e).__name__}: {e}"


def schemas(tools: list[Tool]) -> list[dict]:
    return [t.schema() for t in tools]


def by_name(tools: list[Tool]) -> dict[str, Any]:
    return {t.name: t for t in tools}
