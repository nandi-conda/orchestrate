# Sync server

The server orchestrate control planes on several computers share projects
through: a Cloudflare Worker with a Durable Object per space (Automerge documents,
the change feed, leases, each project's hub as a list of git bundles) and an
R2 bucket (the bundles and pasted images). See
[docs/guide/sync.md](../docs/guide/sync.md) for what is shared and how a
project moves between computers; `src/index.js` documents the API.

It is a public relay: no secret, and every request names its space with
`X-Sync-Space: <key>` (64 hex digits), each space its own Durable Object and
R2 prefix.

```bash
pixi run dev      # http://127.0.0.1:8787, local Durable Object and R2
pixi run deploy   # to your Cloudflare account: wrangler login if needed, the bucket, the Worker
```

The orchestrator's tests (`tests/test_sync.py`) run this server under
`wrangler dev`.
