// The sync server orchestrate control planes share projects through.
//
// A public relay: it keeps no secret and asks for none. Every request names a
// space with `X-Sync-Space: <64 hex digits>`, a random key the control plane
// made (or was given, to pair with another computer). Knowing the key is the
// only way into a space, so it is shared like a secret link. Control planes
// open every connection (they are never reached), so all of it is plain HTTP:
// a sync round, a long-polled change feed, leases, and blobs.
//
// Each space is one Durable Object, SyncHub, and its own prefix in R2:
//
// - Automerge documents, by name ("projects", "project:<id>", "log:<id>:main",
//   "log:<id>:<thread id>"). A control plane syncs one with the Automerge
//   sync protocol: it posts sync messages, the hub applies them and answers
//   with its own. Each round of a sync names a `session`, so the hub keeps
//   one sync state per control plane and document while a sync lasts.
// - A change feed: every document change takes the next `version`, and
//   GET /changes?since=<version> answers with the documents changed since,
//   waiting up to `wait` seconds for one.
// - Leases: each project has one control plane driving it (running its
//   threads); the others only show it and forward what their user does.
//   A lease not renewed for LEASE_STALE ms can be taken; `take` takes it at once.
// - Each project's hub (its git repository) as a list of bundles in R2, each
//   with the full table of refs after it. Only the project's driver adds one.
// - Blobs (pasted images) in R2, each with the JSON its x-meta header gave.

import * as Automerge from "@automerge/automerge";

const LEASE_STALE = 30_000;
const MAX_WAIT = 25;
const PART = 1_000_000; // DO SQLite values stay under 2 MB
const COMPACT_AFTER = 64; // incremental saves kept before a document is saved whole
const SESSION_TTL = 10 * 60_000;
const LOADED_MAX = 200; // documents kept loaded

export default {
  async fetch(request, env) {
    const space = request.headers.get("x-sync-space") || "";
    if (!SPACE.test(space)) {
      return json({ error: "an X-Sync-Space header with the space's key (64 hex digits) is required" }, 400);
    }
    return env.SYNC.get(env.SYNC.idFromName(space)).fetch(request);
  },
};

const SPACE = /^[0-9a-f]{64}$/;

function json(data, status = 200) {
  return new Response(JSON.stringify(data), { status, headers: { "content-type": "application/json" } });
}

function b64decode(s) {
  const bin = atob(s);
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

function b64encode(bytes) {
  let bin = "";
  for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(bin);
}

function sameHeads(a, b) {
  return a.length === b.length && a.every((h, i) => h === b[i]);
}

export class SyncHub {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.sql = ctx.storage.sql;
    this.docs = new Map(); // name -> {doc, chunks}, most recently used last
    this.sessions = new Map(); // `${session}\n${name}` -> {state, at}
    this.waiters = new Set();
    this.sql.exec(`CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v INTEGER NOT NULL)`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS docs (name TEXT PRIMARY KEY, version INTEGER NOT NULL)`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS chunks (name TEXT NOT NULL, n INTEGER NOT NULL,
      part INTEGER NOT NULL, data BLOB NOT NULL, PRIMARY KEY (name, n, part))`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS leases (project TEXT PRIMARY KEY, holder TEXT NOT NULL,
      name TEXT NOT NULL, renewed_at INTEGER NOT NULL)`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS bundles (project TEXT NOT NULL, n INTEGER NOT NULL,
      base TEXT NOT NULL, refs TEXT NOT NULL, at INTEGER NOT NULL, PRIMARY KEY (project, n))`);
  }

  async fetch(request) {
    const url = new URL(request.url);
    const r2 = `${request.headers.get("x-sync-space")}/`; // this space's prefix in R2
    const path = url.pathname.split("/").filter(Boolean);
    const route = `${request.method} /${path[0] || ""}${path.length > 1 ? "/*" : ""}${path.length > 2 ? "/*" : ""}`;
    try {
      switch (route) {
        case "POST /sync":
          return json(this.sync(await request.json()));
        case "GET /changes":
          return json(await this.changes(Number(url.searchParams.get("since") || 0),
            Math.min(Number(url.searchParams.get("wait") || 0), MAX_WAIT)));
        case "POST /lease":
          return json(this.lease(await request.json()));
        case "POST /leases/*":
          if (path[1] === "renew") return json(this.renew(await request.json()));
          if (path[1] === "release") return json(this.release(await request.json()));
          break;
        case "GET /leases":
          return json(this.leases());
        case "GET /hubs/*":
          return json(this.bundles(path[1]));
        case "PUT /hubs/*":
          return await this.addBundle(path[1], request, url, r2);
        case "GET /hubs/*/*":
          return await this.blob(`${r2}hubs/${path[1]}/${Number(path[2])}.bundle`);
        case "PUT /blobs/*":
          await this.env.BUCKET.put(`${r2}blobs/${path[1]}`, await request.arrayBuffer(), {
            httpMetadata: { contentType: request.headers.get("content-type") || "application/octet-stream" },
            customMetadata: { meta: request.headers.get("x-meta") || "{}" },
          });
          return json({ ok: true });
        case "GET /blobs/*":
          return await this.blob(`${r2}blobs/${path[1]}`);
      }
      return json({ error: `no route ${request.method} ${url.pathname}` }, 404);
    } catch (e) {
      return json({ error: `${e.name}: ${e.message}` }, e.status || 500);
    }
  }

  // ------------------------------------------------------------ documents

  load(name) {
    let entry = this.docs.get(name);
    if (entry) {
      this.docs.delete(name);
      this.docs.set(name, entry);
      return entry;
    }
    const rows = this.sql.exec(`SELECT n, data FROM chunks WHERE name = ? ORDER BY n, part`, name).toArray();
    const ns = new Set(rows.map((r) => r.n));
    const bytes = new Uint8Array(rows.reduce((size, r) => size + r.data.byteLength, 0));
    let at = 0;
    for (const r of rows) {
      bytes.set(new Uint8Array(r.data), at);
      at += r.data.byteLength;
    }
    const doc = rows.length ? Automerge.load(bytes) : Automerge.init();
    entry = { doc, chunks: ns.size, next: rows.length ? Math.max(...ns) + 1 : 0 };
    this.docs.set(name, entry);
    while (this.docs.size > LOADED_MAX) this.docs.delete(this.docs.keys().next().value);
    return entry;
  }

  store(name, entry) {
    // An incremental save each change; the whole document once they pile up.
    let data;
    if (entry.chunks >= COMPACT_AFTER) {
      data = Automerge.save(entry.doc);
      this.sql.exec(`DELETE FROM chunks WHERE name = ?`, name);
      entry.chunks = 0;
    } else {
      data = Automerge.saveIncremental(entry.doc);
    }
    for (let part = 0; part * PART < data.length || part === 0; part++) {
      this.sql.exec(`INSERT INTO chunks (name, n, part, data) VALUES (?, ?, ?, ?)`,
        name, entry.next, part, data.subarray(part * PART, (part + 1) * PART));
    }
    entry.next += 1;
    entry.chunks += 1;
    const version = this.bump();
    this.sql.exec(`INSERT INTO docs (name, version) VALUES (?, ?)
      ON CONFLICT (name) DO UPDATE SET version = excluded.version`, name, version);
  }

  bump() {
    const row = this.sql.exec(`SELECT v FROM meta WHERE k = 'version'`).toArray()[0];
    const version = (row ? row.v : 0) + 1;
    this.sql.exec(`INSERT INTO meta (k, v) VALUES ('version', ?) ON CONFLICT (k) DO UPDATE SET v = excluded.v`,
      version);
    return version;
  }

  version() {
    const row = this.sql.exec(`SELECT v FROM meta WHERE k = 'version'`).toArray()[0];
    return row ? row.v : 0;
  }

  // POST /sync {session, messages: [{doc, msg}]} -> {messages: [{doc, msg}]}
  // `msg` is a base64 Automerge sync message; the answer holds one for each
  // document the hub has something more to say about.
  sync({ session, messages }) {
    if (typeof session !== "string" || !Array.isArray(messages)) {
      throw Object.assign(new Error("session and messages are required"), { status: 400 });
    }
    const now = Date.now();
    for (const [key, s] of this.sessions) if (now - s.at > SESSION_TTL) this.sessions.delete(key);
    const out = [];
    let changed = false;
    for (const { doc: name, msg } of messages) {
      if (typeof name !== "string" || !name) continue;
      const key = `${session}\n${name}`;
      const s = this.sessions.get(key) || { state: Automerge.initSyncState() };
      s.at = now;
      const entry = this.load(name);
      if (msg) {
        const before = Automerge.getHeads(entry.doc);
        const [doc, state] = Automerge.receiveSyncMessage(entry.doc, s.state, b64decode(msg));
        entry.doc = doc;
        s.state = state;
        if (!sameHeads(before, Automerge.getHeads(doc))) {
          this.store(name, entry);
          changed = true;
        }
      }
      const [state, reply] = Automerge.generateSyncMessage(entry.doc, s.state);
      s.state = state;
      this.sessions.set(key, s);
      if (reply) out.push({ doc: name, msg: b64encode(reply) });
    }
    if (changed) this.wake();
    return { messages: out };
  }

  // GET /changes?since=<version>&wait=<s> -> {version, docs}: the documents
  // changed after `since`, at once when there are some, else when one is or
  // after `wait` seconds.
  async changes(since, wait) {
    const now = () => ({
      version: this.version(),
      docs: this.sql.exec(`SELECT name FROM docs WHERE version > ? ORDER BY version`, since)
        .toArray().map((r) => r.name),
    });
    const got = now();
    if (got.docs.length || wait <= 0) return got;
    await new Promise((resolve) => {
      const waiter = { resolve };
      this.waiters.add(waiter);
      waiter.timer = setTimeout(() => {
        this.waiters.delete(waiter);
        resolve();
      }, wait * 1000);
    });
    return now();
  }

  wake() {
    for (const w of this.waiters) {
      clearTimeout(w.timer);
      w.resolve();
    }
    this.waiters.clear();
  }

  // ------------------------------------------------------------ leases

  leaseRow(project) {
    return this.sql.exec(`SELECT project, holder, name, renewed_at FROM leases WHERE project = ?`, project)
      .toArray()[0] || null;
  }

  view(row, now = Date.now()) {
    return row && { project: row.project, holder: row.holder, name: row.name, renewed_at: row.renewed_at,
      age: now - row.renewed_at, stale: now - row.renewed_at > LEASE_STALE };
  }

  // POST /lease {project, holder, name, take} -> {granted, lease}: the
  // project's lease goes to `holder` when nobody holds it, its holder has not
  // renewed it for LEASE_STALE ms, `holder` holds it already, or `take`.
  lease({ project, holder, name, take }) {
    if (typeof project !== "string" || typeof holder !== "string" || !project || !holder) {
      throw Object.assign(new Error("project and holder are required"), { status: 400 });
    }
    const now = Date.now();
    const row = this.leaseRow(project);
    const granted = !row || row.holder === holder || take || now - row.renewed_at > LEASE_STALE;
    if (granted) {
      this.sql.exec(`INSERT INTO leases (project, holder, name, renewed_at) VALUES (?, ?, ?, ?)
        ON CONFLICT (project) DO UPDATE SET holder = excluded.holder, name = excluded.name,
        renewed_at = excluded.renewed_at`, project, holder, String(name || holder), now);
    }
    return { granted, lease: this.view(this.leaseRow(project), now) };
  }

  // POST /leases/renew {holder, projects} -> {lost}: renews each of the
  // holder's leases; `lost` lists the projects it no longer holds.
  renew({ holder, projects }) {
    const now = Date.now();
    const lost = [];
    for (const project of projects || []) {
      const row = this.leaseRow(project);
      if (row && row.holder === holder) {
        this.sql.exec(`UPDATE leases SET renewed_at = ? WHERE project = ?`, now, project);
      } else {
        lost.push(project);
      }
    }
    return { lost };
  }

  // POST /leases/release {holder, projects}: lets go of the holder's leases.
  release({ holder, projects }) {
    for (const project of projects || []) {
      this.sql.exec(`DELETE FROM leases WHERE project = ? AND holder = ?`, project, holder);
    }
    return { ok: true };
  }

  leases() {
    const now = Date.now();
    return this.sql.exec(`SELECT project, holder, name, renewed_at FROM leases`).toArray().map((r) => this.view(r, now));
  }

  // ------------------------------------------------------------ hubs and blobs

  bundles(project) {
    return this.sql.exec(`SELECT n, base, refs FROM bundles WHERE project = ? ORDER BY n`, project).toArray()
      .map((r) => ({ n: r.n, base: r.base, refs: JSON.parse(r.refs) }));
  }

  // PUT /hubs/<project>?holder=<holder>: only the project's driver adds to
  // its hub. The body is {base, refs} as JSON (`refs`: every ref of the hub
  // after this bundle), x-meta-length bytes long, then the bundle (empty when
  // only refs moved).
  async addBundle(project, request, url, r2) {
    const holder = url.searchParams.get("holder");
    const row = this.leaseRow(project);
    if (!row || row.holder !== holder) {
      return json({ error: `${holder} does not drive project ${project}` +
        (row ? `; ${row.name} does` : "; nobody does") }, 409);
    }
    const body = new Uint8Array(await request.arrayBuffer());
    const size = Number(request.headers.get("x-meta-length"));
    if (!Number.isInteger(size) || size <= 0 || size > body.length) {
      return json({ error: "x-meta-length must give the length of the {base, refs} JSON the body starts with" }, 400);
    }
    const { base, refs } = JSON.parse(new TextDecoder().decode(body.subarray(0, size)));
    const last = this.sql.exec(`SELECT MAX(n) AS n FROM bundles WHERE project = ?`, project).toArray()[0];
    const n = (last && last.n !== null ? last.n : 0) + 1;
    await this.env.BUCKET.put(`${r2}hubs/${project}/${n}.bundle`, body.subarray(size));
    this.sql.exec(`INSERT INTO bundles (project, n, base, refs, at) VALUES (?, ?, ?, ?, ?)`,
      project, n, String(base || ""), JSON.stringify(refs || {}), Date.now());
    return json({ n });
  }

  async blob(key) {
    const obj = await this.env.BUCKET.get(key);
    if (!obj) return json({ error: `no ${key}` }, 404);
    return new Response(obj.body, {
      headers: { "content-type": obj.httpMetadata?.contentType || "application/octet-stream",
        "x-meta": obj.customMetadata?.meta || "{}" },
    });
  }
}
