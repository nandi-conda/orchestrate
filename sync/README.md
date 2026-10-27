# Sync server

The server orchestrate control planes on several computers share projects
through: a Cloudflare Worker with one Durable Object (Automerge documents,
the change feed, leases, each project's hub as a list of git bundles) and an
R2 bucket (the bundles and pasted images). See
[docs/guide/sync.md](../docs/guide/sync.md) for what is shared and how a
project moves between computers; `src/index.js` documents the API.

```bash
pixi run dev      # http://127.0.0.1:8787, local Durable Object and R2; token in .dev.vars (SYNC_TOKEN=...)
```

Deploy with wrangler, signed in to your Cloudflare account:

```bash
pixi run install                                   # wrangler into node_modules
npx wrangler login
npx wrangler r2 bucket create orchestrate-sync     # once
npx wrangler deploy                                # prints the workers.dev URL
TOKEN=$(openssl rand -hex 32); echo "$TOKEN"       # once: keep it, it is ORCHESTRATE_SYNC_TOKEN
echo "$TOKEN" | npx wrangler secret put SYNC_TOKEN
```

The orchestrator's tests (`tests/test_sync.py`) run this server under
`wrangler dev`.
