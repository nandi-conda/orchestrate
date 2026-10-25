# Runner HTTP API

One runner process per worker (a Sprite, a container, a laptop). It hosts many
threads. Default port 8080. Every route except `/health` needs
`Authorization: Bearer $RUNNER_TOKEN` when `RUNNER_TOKEN` is set.

## Routes

| Method | Path | Body | Returns |
| --- | --- | --- | --- |
| GET | `/health` | | `{"ok", "threads", "running", "capacity"}` |
| GET | `/threads` | | list of directory entries |
| POST | `/threads` | `{"request", "title"?, "project"?, "parent"?, "model"?, "id"?}` | 201 + directory entry; 409 if `id` exists |
| GET | `/threads/{id}` | | directory entry + `"events"` (count) |
| POST | `/threads/{id}/messages` | `{"body", "from"? = "user", "hops"? = 0}` | 202 |
| POST | `/threads/{id}/stop` | | `{"ok": true}` |
| GET | `/threads/{id}/events` | | SSE stream for one thread |
| GET | `/events` | | SSE stream for every thread on this worker |
| GET | `/peer/calls` | `?wait=25` | queued peer calls (below) |
| POST | `/peer/calls/{id}` | `{"ok", "result"}` or `{"ok": false, "error"}` | `{"ok": true}`; 404 if unknown or already answered |

- `id` lets the orchestrator pick thread ids so they match its own records. Omitted, the runner makes `t_<10 hex>`.
- `model` is `{"provider": "deepseek", "model": "deepseek-v4-pro"}` or `{"provider": "openai-compat", "model", "base_url", "api_key_env"}`. Omitted: DeepSeek with `$DEEPSEEK_MODEL`.
- `from` on a message is `"user"` or the sending thread id; non-user messages reach the model as `[from <id>] <body>`.
- A message whose `hops` exceeds the limit (6) is dropped with a `dropped` event. The orchestrator should increment `hops` when it forwards a thread-to-thread message.

### Directory entry

```json
{"id": "t_1a2b3c4d5e", "project": "p", "title": "Fix export", "state": "working",
 "summary": "owns ui/export.py", "files": ["ui/export.py"], "parent": null}
```

`state` is one of `queued`, `working`, `idle`, `failed`, `stopped`. Threads are long-lived: after a turn they go `idle` and wake on the next message.

### SSE

Each event is sent as `id: <seq>`, `event: <type>`, `data: <json>`. Resume with `?after=<seq>` or the `Last-Event-ID` header; seq is per worker and increases across all threads. A `: ping` comment every 15 s keeps proxies open.

```json
{"seq": 12, "thread": "t_1a2b3c4d5e", "type": "tool_call", "ts": 1761339600.1,
 "data": {"id": "call_0", "name": "bash", "arguments": {"command": "pytest -q"}}}
```

| type | data |
| --- | --- |
| `created` | directory entry |
| `state` | `{"state"}` |
| `message_in` | `{"from", "body"}` |
| `assistant` | `{"content"}` |
| `tool_call` | `{"id", "name", "arguments"}` |
| `tool_result` | `{"id", "name", "result"}` (cut to 4000 chars) |
| `usage` | provider token counts |
| `turn_done` | `{"summary", "final"}` |
| `dropped` | `{"from", "reason"}` |
| `error` | `{"error"}` |

## Peer calls (collected by the orchestrator)

The orchestrator is never reachable from a worker, by design: it may run on a
laptop behind NAT, and it opens every connection. The inter-thread tools still
reach it, by queuing their calls on this runner for the orchestrator to collect:

1. `GET /peer/calls?wait=25` (max 55) long-polls. It returns as soon as a call
   is queued, with every call due: `[{"id", "op", "args"}]`, or `[]` after
   `wait` seconds with none.
2. The orchestrator runs each call (concurrently) as this worker, limited to
   the worker's project.
3. `POST /peer/calls/{id}` with `{"ok": true, "result": ...}` or
   `{"ok": false, "error": "..."}` wakes the waiting tool; an error becomes the
   tool's result. 404 if the call is unknown or already answered.

A call handed out but not answered within 60 s is handed out again, so a lost
poll response only delays it. A tool waits up to 15 minutes for its answer
(spawning can wait on a new worker). With no orchestrator polling, peer tools
wait and then fail; the CLI (`pixi run run`) uses in-process peers instead.

| `op` (the tool) | `args` | `result` |
| --- | --- | --- |
| `list_threads` | `{"project", "query"}` (`query` may be null) | list of directory entries |
| `read_thread` | `{"thread_id", "last_n"}` | directory entry + `"events"`: recent event list |
| `send_message` | `{"sender", "to", "body", "reply_to"}` | `{"ok": true}` |
| `spawn_thread` | `{"parent", "project", "title", "request"}` | `{"id", "title"}` |
| `set_summary` | `{"thread_id", "summary", "files"}` | anything |

A thread also calls `set_summary` after each turn, and when it has a `parent`, sends
`Finished: <summary>` to the parent through `send_message`. The orchestrator routes
`send_message` to whichever worker holds `to` (`POST /threads/{to}/messages` with
`hops + 1`), and `spawn_thread` to the least loaded worker.

## Environment

Secrets (`DEEPSEEK_API_KEY`, `RUNNER_TOKEN`, and an `openai-compat` model's `api_key_env`) are read from the environment first, then from the system keyring under service `orchestrate` with the variable name as the username (`pixi run set-key DEEPSEEK_API_KEY`). An item added by hand in a keyring GUI is found too when its label is the variable name; only unlocked collections are searched. `serve` and `run` print the missing ones, with the keyring backend in use, and exit if a required secret is in neither place.

| Var | Default | Meaning |
| --- | --- | --- |
| `DEEPSEEK_API_KEY` | | required for DeepSeek, sent as `Authorization: Bearer` |
| `DEEPSEEK_MODEL` | `deepseek-v4-pro` | default model |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` | |
| `RUNNER_TOKEN` | | bearer token the orchestrator uses to call this runner |
| `RUNNER_PORT` / `RUNNER_HOST` | `8080` / `0.0.0.0` | |
| `RUNNER_REPO` | | git repo to make one worktree per thread from (branch `thread/<id>`) |
| `RUNNER_WORKDIR` | `/work/threads` | where workspaces go |
| `RUNNER_CAPACITY` | `16` | reported in `/health` for scheduling |
| `RUNNER_MAX_STEPS` | `60` | model calls per turn |
