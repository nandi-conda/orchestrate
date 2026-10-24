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
| GET | `/peer/calls` | `?wait=25` (max 55) | pending peer calls (pull mode, below); `[]` after `wait` seconds with none |
| POST | `/peer/calls/{id}` | `{"status", "body"}` | `{"ok": true}`; 404 if the call is unknown or already answered |

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

## Orchestrator peer API (called by the runner)

When `ORCHESTRATOR_URL` is set, the inter-thread tools call the orchestrator with
`Authorization: Bearer $ORCHESTRATOR_TOKEN`. Without it, the tools only see threads
on the same runner.

| Tool | Method | Path | Body / params | Expected reply |
| --- | --- | --- | --- | --- |
| `list_threads` | GET | `/api/peer/projects/{project}/threads` | `?q=` optional | list of directory entries |
| `read_thread` | GET | `/api/peer/threads/{id}` | `?last_n=20` | directory entry + `"events"`: recent event list |
| `send_message` | POST | `/api/peer/messages` | `{"from", "to", "body", "reply_to"}` | `{"ok": true}` |
| `spawn_thread` | POST | `/api/peer/projects/{project}/threads` | `{"parent", "title", "request"}` | `{"id", "title"}` |
| `set_summary` | PUT | `/api/peer/threads/{id}/summary` | `{"summary", "files"}` | anything |

### Pull mode

When the worker cannot reach the orchestrator (a control plane on a laptop,
workers on Sprites), start it with `RUNNER_PEERS=pull` instead of
`ORCHESTRATOR_URL`. The tools then queue their calls, and the orchestrator,
which can reach the worker, collects them over plain HTTP:

1. `GET /peer/calls?wait=25` long-polls and returns
   `[{"id", "method", "path", "params", "json"}]`, with `path` one of the peer
   routes in the table.
2. The orchestrator runs each call against its own peer API as this worker.
3. `POST /peer/calls/{id}` with `{"status": <http status>, "body": <json reply>}`
   wakes the waiting tool. A status of 400 or more becomes a tool error.

A call that is handed out but not answered within 60 s is handed out again, so
a lost poll response only delays it. A tool waits up to 15 minutes for its answer.

A thread also calls `set_summary` after each turn, and when it has a `parent`, sends
`Finished: <summary>` to the parent through `send_message`. The orchestrator routes
`send_message` to whichever worker holds `to` (`POST /threads/{to}/messages` with
`hops + 1`), and `spawn_thread` to the least loaded worker.

## Environment

Secrets (`DEEPSEEK_API_KEY`, `RUNNER_TOKEN`, `ORCHESTRATOR_TOKEN`, and an `openai-compat` model's `api_key_env`) are read from the environment first, then from the system keyring under service `orchestrate` with the variable name as the username (`pixi run set-key DEEPSEEK_API_KEY`). An item added by hand in a keyring GUI is found too when its label is the variable name; only unlocked collections are searched. `serve` and `run` print the missing ones, with the keyring backend in use, and exit if a required secret is in neither place.

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
| `ORCHESTRATOR_URL` / `ORCHESTRATOR_TOKEN` | | peer API above |
| `RUNNER_PEERS` | | `pull`: serve peer calls on `/peer/calls` instead of dialling `ORCHESTRATOR_URL` |
