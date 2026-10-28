---
name: team
description: Work with the other threads of this project and with the orchestrator - move your objectives on and keep your summary current, list and read threads, message them, spawn a thread for independent work, and search and fetch the web.
---

# Team

You are one thread of a team working on one project. The `team` module is
prepared in the Python kernel; every function is async.

```python
mine = await team.objectives()                   # [{"id": "o_3f9a1c2b", "text": "Find why Safari ...", "status": "pending", "note": ""}, ...]
await team.update_objective(mine[0]["id"], "reproduced: the click handler is never bound on Safari")
await goal.complete()                            # your current objective is achieved: the runner marks it done
rows = await team.list_threads()                 # the other threads: id, title, state, summary, files
await team.send_message("t_1a2b3c4d5e", "I'm changing ui/export.py; hold off on it.")
child = await team.spawn_thread("Fix Safari export", "The export button does nothing on Safari...")
await team.set_summary("Owns ui/export.py: rewriting the export flow.")
results = await team.web_search("bubblewrap user namespace proc mount", question="why does bwrap fail to mount proc")
page = await team.web_fetch(results[0]["url"])
```

## API

- `objectives() -> list[dict]`: your objectives, in the order to work them: `{"id", "text", "status", "note"}`, `status` `pending`, `in_progress` or `done`, `note` a short line on where it is at. The orchestrator makes them and delegates each to one thread; yours come with your request and with messages that bring more work.
- `update_objective(id: str, note: str) -> list[dict]`: your note on where an objective is at. You cannot add or reword objectives, or set their status: the runner sets your first objective not done as your prime-agent goal (`in_progress`), and `await goal.complete()` marks it `done`.
- `list_threads(query: str | None = None) -> list[dict]`: the project's other threads, as directory entries (`id`, `title`, `state`, `summary`, `objectives`, `files`, `parent`).
- `read_thread(id: str, last_n: int = 20) -> dict`: a thread's directory entry and its recent events.
- `send_message(to: str, body: str, reply_to: str | None = None) -> dict`: put a message in another thread's inbox: hand off work, ask a question, warn about a conflict.
- `spawn_thread(title: str, request: str) -> dict`: start a new thread for an independent piece of work (`{"id", "title"}`). Prefer it over doing unrelated work yourself; its result comes back to you as a message.
- `set_summary(summary: str) -> None`: your one-paragraph status in the thread directory, so other threads and the router know what you own.
- `web_search(query: str, count: int = 10, question: str | None = None) -> list[dict]`: ranked results (`title`, `url`, `date`, `summary`). `query` is at most 128 characters and takes `site:`, `-site:`, `"exact phrase"`, `-term`, `lang:en`, `after:2025-01-01`, `before:2026`; `question` is the question behind it, at most 128 characters.
- `web_fetch(url: str) -> dict`: a page as Markdown (`title`, `content`).

A failed call raises `team.TeamError` with the reason.

You work your objectives in order, one at a time, as your goal: while it is open the runner keeps
giving you turns on it. Call `await goal.complete()` only once it is achieved, never for anything
else, and keep its note current: the person watches them. A turn that ends with every objective `done` finishes the thread: it
ends and is archived, and takes no more messages.
