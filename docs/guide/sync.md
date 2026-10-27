# Projects on several computers

Every computer that runs orchestrate (the desktop window, `pixi run serve`, the
Android app) syncs its projects through a **sync server**, a public relay on
Cloudflare. Computers in the same **space** share every project, live: its
chat, its threads and what they do. Open a project on the laptop that you
started on the desktop, and go on there.

## Spaces

There is nothing to set up for one computer: the first time it starts, it makes
its own space, a random key kept in its data folder (`~/.orchestrate/space`),
and its projects are synced from then on.

To share them with another computer, pair it:

1. On the first computer, open **Settings** on the project list and **Copy** the
   space key.
2. On the other computer, set it as `ORCHESTRATE_SYNC_SPACE`: **Set…** next to it
   in Settings, `pixi run set-key ORCHESTRATE_SYNC_SPACE`, the environment, or the
   Android app's Keys screen. Restart orchestrate there.

It is then in the first computer's space, and the projects it had are shared
there too. The control plane sends the key as `X-Sync-Space: <key>` on every
request. The relay keeps no secret and checks no account: the key is the only
way into a space, so share it like a secret link. Anyone with it can see and
change the space's projects, and whoever runs the relay can read them (they are
not encrypted end to end).

A computer that cannot reach the relay starts all the same and catches up once
it can; until then it shows what it had and does not drive projects.

## The server

orchestrate uses `https://orchestrate-sync.nandithebull.workers.dev`;
`ORCHESTRATE_SYNC_URL` names another. To run your own on your Cloudflare account,
one command from a checkout signs in with wrangler (the browser opens the first
time), makes the R2 bucket and deploys the Worker, and is safe to run again:

```bash
pixi run deploy-sync
```

When its URL is not the default, it keeps it in this computer's keyring as
`ORCHESTRATE_SYNC_URL`; set the same on your other computers.

## Who drives a project

One computer **drives** each project at a time: it routes the project's
requests, runs its threads on its own workers, and keeps its git hub. The
project's header on every other computer says **Driven by** that computer.

- What you do on a computer that does not drive the project (send a request,
  message a thread, edit a message, stop, archive, change settings) is handed to
  the computer that does, which does it as if you had done it there. Its result
  shows on every computer a moment later.
- A computer drives a project when you open or use it there and no computer
  drives it: nobody did, or the one that did stopped (it quit, or has not
  renewed its claim for half a minute).
- **Take over** in the project's header drives it from this computer at once.
  The computer that drove it stops the project's threads on its workers; each
  comes back on this computer's workers with its next message (or Respawn), from
  its branch, files and agent session as its last turn left them. A turn under
  way on the other computer is cut short there and goes on from its last saved turn.

Workers belong to the computer that runs them: worker logs and a thread's
Files tab are on the computer that drives the project.

## What is shared

| What | How |
| --- | --- |
| Projects, threads, messages between threads | Automerge documents, one per project |
| The project chat and each thread's events | Logs: on the server, a list of entries per thread, each event added once and fetched a page at a time, so a log can be any size (a reply streaming in shows on the driving computer only, until it ends) |
| The project's hub: its main branch, each thread's branch, files and agent session | git bundles in R2, one per saved turn |
| Pasted images | R2 |

Each computer keeps its own database and data in `~/.orchestrate` as before,
plus `sync.db`, its copy of the documents. Workers, worker logs and
per-computer settings are not shared.

A project opened from a folder keeps that folder as its remote on the computer
it is on. Another computer that drives the project lands threads' work in its
hub, and the work reaches the folder when the folder's computer drives the
project again.

## How the relay works

`sync/` is a Cloudflare Worker with a Durable Object per space, which holds the
space's documents (small: only the few Automerge ones are ever in its memory), its logs, its change feed, the leases (which computer drives which
project) and each hub's list of bundles, and an R2 bucket for the bundles and
images, under the space's prefix. Control planes reach it over HTTPS and nothing
ever connects to them, as with workers. `pixi run dev` in `sync/` serves it
locally, and `orchestrator/tests/test_sync.py` runs control planes against it.
