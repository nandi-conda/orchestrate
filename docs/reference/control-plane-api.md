# Control plane HTTP API

The API the UI uses, served by `orchestrator/control/app.py` on
`http://127.0.0.1:8000` by default. Bodies are JSON. It has **no
authentication**: keep it on loopback unless the network in front of it is
trusted. Workers never call it.

Errors are `{"error": "<reason>"}` with 400 (bad input, or a remote git
refused), 404 (no such project or thread), 409 (the project or thread is
archived, or the project is at its running-thread limit) or 502 (a worker or
provider call failed).

## Projects

| Method | Path | Body / query | Returns |
| --- | --- | --- | --- |
| GET | `/api/projects` | `?archived=1` for archived ones | list of projects; each has `repo_path` (its hub) and `repo_url` (its remote, `""` for none) |
| POST | `/api/projects` | `{name, model?}`, or `{folder, name?, model?}` to open a folder on disk (its full path; named after it unless `name` is given) | 201 + the project; a folder that cannot be opened is a 400 with the reason, and nothing is made |
| GET | `/api/folders` | `?path=` a full path (`~` works), the home folder by default | `{path, parent, entries: [{name, path, git}]}`: the folders in it (hidden ones left out), for the folder picker; `parent` is `""` at the top. A path that is not a readable folder is a 400 |
| GET | `/api/projects/{id}` | | the project |
| POST | `/api/projects/{id}/settings` | any of `{name, model, repo_url}`; `repo_url: ""` removes the remote | the project; a remote the hub cannot join is a 400 with git's reason, and nothing changes |
| POST | `/api/projects/{id}/archive` | | the project: running threads stopped, workers destroyed |
| POST | `/api/projects/{id}/unarchive` | | the project |

## Requests and messages

| Method | Path | Body | Returns |
| --- | --- | --- | --- |
| POST | `/api/projects/{id}/requests` | `{text?, images?, mode: auto\|thread\|new, target?}` | returns at once. `auto` → 202 `{request, mode, queued, seq}`, the plan follows as a `route` event; `thread` (to `target`) and `new` → 200 + the plan. A request that is only `images` skips the router and starts one thread |
| POST | `/api/projects/{id}/messages/{seq}/edit` | `{text}` | edits your request or direct message with that event `seq`; 202 `{changed: true, ...}` and re-assessed in the background, or 200 `{changed: false}` |
| POST | `/api/threads/{id}/messages` | `{body, images?}` | 202 `{ok}`; delivered once the thread is on a worker |

`images` are `data:` URLs (`image/png`, `jpeg`, `webp` or `gif`), at most 4
per message and 8 MB each. The control plane stores their bytes in its
`images` table and logs only a reference (`{id, media_type, name, size}`). When
a message is delivered, the images are inlined as base64 for the runner.

## Threads

| Method | Path | Body / query | Returns |
| --- | --- | --- | --- |
| GET | `/api/projects/{id}/threads` | `?archived=1` for archived ones | the thread directory, each thread with its [load](../architecture/routing.md#thread-load) |
| POST | `/api/projects/{id}/threads` | `{request?, images?, title?}` | 201 + a new thread, bypassing the router |
| GET | `/api/threads/{id}` | | the thread, with its state, binding and reason, and its load |
| POST | `/api/threads/{id}/stop` | | `{ok}` |
| POST | `/api/threads/{id}/respawn` | | 202 `{ok}`; the outcome arrives as the thread's `binding` events ([worker lifecycle](../worker-lifecycle.md)) |
| POST | `/api/threads/{id}/archive` | | the thread, stopped first if running |
| POST | `/api/threads/{id}/unarchive` | | the thread |

## Logs, events, files

| Method | Path | Query | Returns |
| --- | --- | --- | --- |
| GET | `/api/projects/{id}/log` | | the project's newest events (JSON), oldest first |
| GET | `/api/projects/{id}/events` | `?after=seq` or `Last-Event-ID` | SSE: every event in the project |
| GET | `/api/threads/{id}/transcript` | | the thread's newest events (JSON) |
| GET | `/api/threads/{id}/events` | `?after=seq` or `Last-Event-ID` | SSE: the thread's events |
| GET | `/api/threads/{id}/worker-logs` | `?lines=300` (max 5000) | text: the thread's binding and worker state, then its worker's runner log |
| GET | `/api/threads/{id}/files` | `?path=<dir>` | `{path, entries: [{name, dir, size}]}` of the thread's worktree |
| GET | `/api/threads/{id}/files/raw` | `?path=<file>` | the file: an image (png, jpg, gif, webp, svg) inline as its type, anything else as an attachment; served with a sandbox CSP and `nosniff`, so nothing in it runs |
| GET | `/api/images/{id}` | | a pasted image's bytes, cacheable for good |
| GET | `/health` | | the control plane is up |

Reading a thread's files when it is on no worker binds it to one first.

### Streams

SSE streams send `id: <seq>`, `event: <type>`, `data: <json>`, one frame per
event, except a reply still streaming: its `delta` events fold into the row
they end up as, one per part (the reasoning, the reply, each tool call), under
the `seq` the part started at, with `live: 1`, the `text` so far, and
`began: 1` on the frame that started the part. The UI grows that row instead
of refetching the pane; the event that ends the model call replaces it.
