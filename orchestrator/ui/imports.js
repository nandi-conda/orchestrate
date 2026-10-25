// Extra imports for ui/orch.py, passed to ultra's mount(..., { imports }).
import { parse } from "./markdown.js";

export function imports({ str, strs, vals, dispatch }) {
  const put = (v) => { vals.push(v); return vals.length - 1; };
  const streams = new Map();
  let nextStream = 1;
  let doc = { op: [], s: [] }; // the markdown md_parse last parsed
  return {
    o_new: () => put({}),
    o_str: (o, k, h) => { vals[o][str(k)] = strs[h] ?? ""; },
    o_lit: (o, k, v) => { vals[o][str(k)] = str(v); },
    http_post: (u, o, msg) => {
      const h = put(null);
      fetch(strs[u], { method: "POST", headers: { "content-type": "application/json", accept: "application/json" },
                       body: JSON.stringify(vals[o] ?? {}) })
        .then(async (r) => {
          const body = await r.json().catch(() => null);
          vals[h] = r.ok ? body : { __error: (body && (body.error || body.message)) || "HTTP " + r.status };
        })
        .catch((e) => { vals[h] = { __error: String(e.message || e) }; })
        .then(() => { vals[o] = undefined; dispatch(msg, h); });
      return h;
    },
    http_get_text: (u, msg) => {
      const h = put(null);
      fetch(strs[u], { headers: { accept: "text/plain" } })
        .then(async (r) => {
          const body = await r.text();
          if (r.ok) { vals[h] = { text: body }; return; }
          let err = body.trim();
          try { err = JSON.parse(body).error || err; } catch {}
          vals[h] = { __error: err || "HTTP " + r.status };
        })
        .catch((e) => { vals[h] = { __error: String(e.message || e) }; })
        .then(() => dispatch(msg, h));
      return h;
    },
    sse_open: (u, msg) => {
      const id = nextStream++;
      const es = new EventSource(strs[u]);
      es.onmessage = (e) => { try { dispatch(msg, put(JSON.parse(e.data))); } catch {} };
      // Typed events (event: <type>) don't reach onmessage; listen to the ones we emit.
      for (const t of ["request", "created", "state", "message_in", "message_out", "delta", "reasoning", "assistant", "tool_call",
                       "tool_result", "usage", "turn_done", "dropped", "error", "route", "worker",
                       "saved", "landed", "land_conflict",
                       "archived", "unarchived"])
        es.addEventListener(t, es.onmessage);
      streams.set(id, es);
      return id;
    },
    sse_close: (id) => { const es = streams.get(id); if (es) { es.close(); streams.delete(id); } },
    page_project: (d) => {
      const m = location.pathname.match(/^\/p\/([^/]+)/);
      if (!m) return 0;
      strs[d] = decodeURIComponent(m[1]);
      return 1;
    },
    nav_push: (u) => { history.pushState(null, "", strs[u]); },
    on_nav: (msg) => { addEventListener("popstate", () => dispatch(msg, 0)); },
    set_title: (h) => { document.title = strs[h]; },
    md_parse: (h) => { doc = parse(strs[h] ?? ""); return doc.op.length; },
    md_op: (i) => doc.op[i],
    md_str: (i, d) => { strs[d] = doc.s[i]; },
    scroll_end: (id) => {
      const sel = str(id);
      requestAnimationFrame(() => requestAnimationFrame(() => {
        const el = document.getElementById(sel);
        if (el) el.scrollTop = el.scrollHeight;
      }));
    },
  };
}
