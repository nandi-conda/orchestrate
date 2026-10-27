# Sync server

The server orchestrate control planes on several computers share projects
through: a Cloudflare Worker with one Durable Object (Automerge documents,
the change feed, leases, each project's hub as a list of git bundles) and an
R2 bucket (the bundles and pasted images). See
[docs/guide/sync.md](../docs/guide/sync.md) for what is shared and how a
project moves between computers; `src/index.js` documents the API.

```bash
pixi run dev      # http://127.0.0.1:8787, local Durable Object and R2; token in .dev.vars (SYNC_TOKEN=...)
pixi run deploy   # to your Cloudflare account; prints ORCHESTRATE_SYNC_URL and, the first time, ORCHESTRATE_SYNC_TOKEN
```

`deploy` needs `CLOUDFLARE_API_TOKEN` (an API token with **Account > Workers
Scripts: Edit** and **Account > Workers R2 Storage: Edit**) and
`CLOUDFLARE_ACCOUNT_ID`. The orchestrator's tests (`tests/test_sync.py`) run
this server under `wrangler dev`.
