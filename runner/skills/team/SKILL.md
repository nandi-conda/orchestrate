---
name: team
description: Work with the other threads of this project and with the orchestrator - keep your objectives and summary current, list and read threads, message them, spawn a thread for independent work, and search and fetch the web.
---

# Team

You are one thread of a team working on one project. The `team` module is
prepared in the Python kernel; every function is async.

```python
await team.set_objectives(["Find why Safari ignores the export click", "Fix it and add a test"])
await team.update_objective(0, "in_progress", "reproduced: the click handler is never bound on Safari")
await team.update_objective(0, "done", "the handler binds on pointerup, which Safari fires")
rows = await team.list_threads()                 # the other threads: id, title, state, summary, files
await team.send_message("t_1a2b3c4d5e", "I'm changing ui/export.py; hold off on it.")
child = await team.spawn_thread("Fix Safari export", "The export button does nothing on Safari...")
await team.set_summary("Owns ui/export.py: rewriting the export flow.")
results = await team.web_search("bubblewrap user namespace proc mount", question="why does bwrap fail to mount proc")
page = await team.web_fetch(results[0]["url"])
```

## API

- `set_objectives(objectives: list) -> list[dict]`: replace your objectives, the things this thread is trying to accomplish. Each is a string (a new, `pending` objective) or `{"text", "status", "note"}`: `status` is `pending`, `in_progress` or `done`, `note` a short line on where it is at. One to 20 of them. Returns them as kept.
- `update_objective(index: int, status: str | None = None, note: str | None = None, text: str | None = None) -> list[dict]`: change objective `index` (from 0); what you leave out stays.
- `objectives() -> list[dict]`: your objectives as they stand.
- `list_threads(query: str | None = None) -> list[dict]`: the project's other threads, as directory entries (`id`, `title`, `state`, `summary`, `objectives`, `files`, `parent`).
- `read_thread(id: str, last_n: int = 20) -> dict`: a thread's directory entry and its recent events.
- `send_message(to: str, body: str, reply_to: str | None = None) -> dict`: put a message in another thread's inbox: hand off work, ask a question, warn about a conflict.
- `spawn_thread(title: str, request: str) -> dict`: start a new thread for an independent piece of work (`{"id", "title"}`). Prefer it over doing unrelated work yourself; its result comes back to you as a message.
- `set_summary(summary: str) -> None`: your one-paragraph status in the thread directory, so other threads and the router know what you own.
- `web_search(query: str, count: int = 10, question: str | None = None) -> list[dict]`: ranked results (`title`, `url`, `date`, `summary`). `query` is at most 128 characters and takes `site:`, `-site:`, `"exact phrase"`, `-term`, `lang:en`, `after:2025-01-01`, `before:2026`; `question` is the question behind it, at most 128 characters.
- `web_fetch(url: str) -> dict`: a page as Markdown (`title`, `content`).

A failed call raises `team.TeamError` with the reason.

Set your objectives at the start of your first turn, from your request, and keep them current:
the person watches them. A turn that ends with every objective `done` finishes the thread: it
ends and is archived, and takes no more messages.
