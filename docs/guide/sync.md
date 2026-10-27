# Projects on several computers

Every computer that runs orchestrate (the desktop window, `pixi run serve`, the
Android app) can share its projects with your other computers through a **sync
server** you deploy to your own Cloudflare account. Each computer then shows
every project, live: its chat, its threads and what they do. Open a project on
the laptop that you started on the desktop, and go on there.

## Set it up

1. Make a Cloudflare API token with **Account > Workers Scripts: Edit** and
   **Account > Workers R2 Storage: Edit** (the *Edit Cloudflare Workers* template
   has both), and note your account id.
2. Deploy the server from a checkout:

    ```bash
    export CLOUDFLARE_API_TOKEN=... CLOUDFLARE_ACCOUNT_ID=...
    cd sync && pixi run deploy
    ```

    It makes the R2 bucket `orchestrate-sync`, deploys the Worker, and prints:

    ```
    ORCHESTRATE_SYNC_URL=https://orchestrate-sync.<you>.workers.dev
    ORCHESTRATE_SYNC_TOKEN=<token>
    ```

    The token is shown once. Deploying again keeps it; `pixi run deploy --new-token`
    makes a new one, which every computer then needs.
3. Set both on every computer, as environment variables or in the keyring
   (`pixi run set-key ORCHESTRATE_SYNC_URL`, `pixi run set-key ORCHESTRATE_SYNC_TOKEN`),
   or on the Android app's Keys screen. The control plane sends the token as
   `Authorization: Bearer <token>` on every request to the server.

The first time a computer syncs, the projects it already has are shared from it.

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
| The project chat and each thread's events | Automerge documents, one per thread (a reply streaming in shows on the driving computer only, until it ends) |
| The project's hub: its main branch, each thread's branch, files and agent session | git bundles in R2, one per saved turn |
| Pasted images | R2 |

Each computer keeps its own database and data in `~/.orchestrate` as before,
plus `sync.db`, its copy of the documents. Workers, worker logs and
per-computer settings are not shared.

A project opened from a folder keeps that folder as its remote on the computer
it is on. Another computer that drives the project lands threads' work in its
hub, and the work reaches the folder when the folder's computer drives the
project again.

## The server

`sync/` is a Cloudflare Worker with one Durable Object, which holds the
documents, the change feed, the leases (which computer drives which project)
and each hub's list of bundles, and an R2 bucket for the bundles and images.
Control planes reach it over HTTPS and nothing ever connects to them, as with
workers. `pixi run dev` in `sync/` serves it locally (token in `sync/.dev.vars`,
`SYNC_TOKEN=...`), and `orchestrator/tests/test_sync.py` runs two control
planes against it.
