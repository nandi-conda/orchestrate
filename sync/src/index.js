// The sync server orchestrate control planes share projects through.
//
// A public relay: it keeps no secret and asks for none. Every request names a
// space with `X-Sync-Space: <64 hex digits>`, a random key the control plane
// made (or was given, to pair with another computer). Knowing the key is the
// only way into a space, so it is shared like a secret link. Control planes
// open every connection (they are never reached), so all of it is plain HTTP:
// a sync round, a long-polled change feed, logs, leases, and blobs.
//
// Each space is one Durable Object, SyncHub, and its own prefix in R2:
//
// - Automerge documents, by name ("projects", "project:<id>"): small, and
//   changed by any computer. A control plane syncs one with the Automerge
//   sync protocol: it posts sync messages, the hub applies them and answers
//   with its own. Each round of a sync names a `session`, so the hub keeps
//   one sync state per control plane and document while a sync lasts.
// - Logs ("log:<id>:main", "log:<id>:<thread id>"): a project's events, which
//   only its driver writes and nobody changes, so they need no merging and
//   can be any size. Each is a list of entries, each a uid and its row (JSON
//   text, kept in parts), added once (POST /logs/<log>) and read a page at a
//   time (GET /logs/<log>?after=<n>). Nothing holds a whole log in memory.
// - A change feed: every change of a document or a log takes the next
//   `version`, and GET /changes?since=<version> answers with the names of
//   those changed since, waiting up to `wait` seconds for one.
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
const LOADED_MAX = 16; // documents kept loaded (a Worker has 128 MB)
const PAGE_BYTES = 4_000_000; // of log rows, per GET /logs answer
const PAGE_ENTRIES = 1000;
const TEXT_PART = 500_000; // UTF-16 units: at most 1.5 MB of UTF-8, under a DO SQLite value's 2 MB

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
    this.sql.exec(`CREATE TABLE IF NOT EXISTS entries (log TEXT NOT NULL, n INTEGER NOT NULL, uid TEXT NOT NULL,
      size INTEGER NOT NULL, PRIMARY KEY (log, n), UNIQUE (log, uid))`);
    this.sql.exec(`CREATE TABLE IF NOT EXISTS entry_parts (log TEXT NOT NULL, n INTEGER NOT NULL,
      part INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY (log, n, part))`);
    // Logs were Automerge documents once: those are dropped, and their
    // computers send the events again, as entries.
    this.sql.exec(`DELETE FROM chunks WHERE name LIKE 'log:%'`);
    this.sql.exec(`DELETE FROM docs WHERE name LIKE 'log:%' AND name NOT IN (SELECT DISTINCT log FROM entries)`);
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
        case "POST /logs/*":
          return json(this.append(decodeURIComponent(path[1]), await request.json()));
        case "GET /logs/*":
          return json(this.entries(decodeURIComponent(path[1]), Number(url.searchParams.get("after") || 0)));
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

  // ------------------------------------------------------------ logs

  // POST /logs/<log> {entries: [{uid, row}]} -> {n}: entries not there yet
  // (by uid) are added, in order; `n` is the log's last.
  append(log, { entries }) {
    if (!log.startsWith("log:") || !Array.isArray(entries)) {
      throw Object.assign(new Error("a log:<project>:<thread> name and entries are required"), { status: 400 });
    }
    const last = () => this.sql.exec(`SELECT MAX(n) AS n FROM entries WHERE log = ?`, log).toArray()[0].n || 0;
    let n = last();
    let added = 0;
    for (const { uid, row } of entries) {
      if (typeof uid !== "string" || typeof row !== "string") continue;
      if (this.sql.exec(`SELECT 1 FROM entries WHERE log = ? AND uid = ?`, log, uid).toArray().length) continue;
      n += 1;
      this.sql.exec(`INSERT INTO entries (log, n, uid, size) VALUES (?, ?, ?, ?)`, log, n, uid, row.length);
      for (let part = 0; part * TEXT_PART < row.length || part === 0; part++) {
        this.sql.exec(`INSERT INTO entry_parts (log, n, part, data) VALUES (?, ?, ?, ?)`,
          log, n, part, row.slice(part * TEXT_PART, (part + 1) * TEXT_PART));
      }
      added += 1;
    }
    if (added) {
      this.sql.exec(`INSERT INTO docs (name, version) VALUES (?, ?)
        ON CONFLICT (name) DO UPDATE SET version = excluded.version`, log, this.bump());
      this.wake();
    }
    return { n };
  }

  // GET /logs/<log>?after=<n> -> {entries: [{n, uid, row}], more}: the
  // entries after `n`, at most PAGE_ENTRIES and PAGE_BYTES of them (at least one).
  entries(log, after) {
    const out = [];
    let bytes = 0;
    const page = this.sql.exec(`SELECT n, uid, size FROM entries WHERE log = ? AND n > ? ORDER BY n LIMIT ?`,
      log, after, PAGE_ENTRIES + 1).toArray();
    let more = page.length > PAGE_ENTRIES;
    for (const e of page.slice(0, PAGE_ENTRIES)) {
      if (out.length && bytes + e.size > PAGE_BYTES) {
        more = true;
        break;
      }
      const row = this.sql.exec(`SELECT data FROM entry_parts WHERE log = ? AND n = ? ORDER BY part`, log, e.n)
        .toArray().map((p) => p.data).join("");
      out.push({ n: e.n, uid: e.uid, row });
      bytes += e.size;
    }
    return { entries: out, more };
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
    // The body is streamed to R2 past its meta: a bundle can be bigger than a Worker's memory.
    const size = Number(request.headers.get("x-meta-length"));
    const total = Number(request.headers.get("content-length"));
    if (!Number.isInteger(size) || size <= 0 || !Number.isInteger(total) || size > total) {
      return json({ error: "x-meta-length must give the length of the {base, refs} JSON the body starts with, " +
        "and content-length the body's" }, 400);
    }
    const reader = request.body.getReader();
    const head = new Uint8Array(size);
    let have = 0;
    let rest = null;
    while (have < size) {
      const { done, value } = await reader.read();
      if (done) return json({ error: "the body ended inside its meta" }, 400);
      const take = Math.min(value.length, size - have);
      head.set(value.subarray(0, take), have);
      have += take;
      if (take < value.length) rest = value.subarray(take);
    }
    const { base, refs } = JSON.parse(new TextDecoder().decode(head));
    const last = this.sql.exec(`SELECT MAX(n) AS n FROM bundles WHERE project = ?`, project).toArray()[0];
    const n = (last && last.n !== null ? last.n : 0) + 1;
    const bundle = new FixedLengthStream(total - size);
    const piping = (async () => {
      const writer = bundle.writable.getWriter();
      if (rest) await writer.write(rest);
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        await writer.write(value);
      }
      await writer.close();
    })();
    await Promise.all([this.env.BUCKET.put(`${r2}hubs/${project}/${n}.bundle`, bundle.readable), piping]);
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
