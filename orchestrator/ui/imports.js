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
    s_add_enc: (d, h) => { strs[d] = (strs[d] ?? "") + encodeURIComponent(strs[h] ?? ""); },
    is_md: (h) => /\.(md|markdown)$/i.test(strs[h] ?? "") ? 1 : 0,
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
    // Copy: one path, the async Clipboard API. A click is a user gesture and
    // the page is served from localhost (a secure context), in a browser and
    // in the desktop window alike.
    clip_copy: (h, ok, arg, fail) => {
      navigator.clipboard.writeText(strs[h] ?? "").then(
        () => dispatch(ok, arg),
        (e) => { strs[h] = String(e && e.message || e); dispatch(fail, h); });
    },
    // Text selected inside the pane (#pane): its text goes to th, a style
    // that puts the Reply button over it to sh, and update(msg, 1). When the
    // selection goes away (or leaves the pane), update(msg, 0).
    on_select: (th, sh, msg) => {
      let last = "", queued = false;
      const inPane = (n) => !!(n && (n.nodeType === 1 ? n : n.parentElement)?.closest("#pane"));
      const check = () => {
        queued = false;
        const sel = getSelection();
        let txt = "", style = "";
        if (sel && !sel.isCollapsed && sel.rangeCount > 0) {
          const r = sel.getRangeAt(0);
          if (inPane(r.startContainer) && inPane(r.endContainer)) {
            txt = sel.toString().trim();
            const b = r.getBoundingClientRect();
            const pane = document.getElementById("pane").getBoundingClientRect();
            const x = Math.round(Math.min(Math.max(b.left + b.width / 2, pane.left + 48), pane.right - 48));
            // Above the selection, or below it when that would leave the pane.
            const above = b.top - 40 >= pane.top;
            const y = Math.round(above ? b.top - 40 : Math.min(b.bottom + 6, pane.bottom - 36));
            style = `top:${y}px;left:${x}px`;
          }
        }
        const k = txt ? txt + "\u0000" + style : "";
        if (k === last) return;
        last = k;
        strs[th] = txt;
        strs[sh] = style;
        dispatch(msg, txt ? 1 : 0);
      };
      const soon = () => { if (!queued) { queued = true; requestAnimationFrame(check); } };
      document.addEventListener("selectionchange", soon);
      document.addEventListener("scroll", soon, true);
      addEventListener("resize", soon);
      // Pressing the Reply button must not collapse the selection it acts on.
      document.addEventListener("mousedown", (e) => {
        if (e.target.closest && e.target.closest(".sel-reply")) e.preventDefault();
      }, true);
    },
    sel_clear: () => { getSelection().removeAllRanges(); },
    // dst = each line of src as a markdown quote ("> line").
    s_quote: (d, h) => { strs[d] = (strs[h] ?? "").split("\n").map((l) => (l ? "> " + l : ">")).join("\n"); },
    focus_end: (id) => {
      const sel = str(id);
      requestAnimationFrame(() => {
        const el = document.getElementById(sel);
        if (!el) return;
        el.focus();
        el.setSelectionRange(el.value.length, el.value.length);
      });
    },
    scroll_end: (id) => {
      const sel = str(id);
      requestAnimationFrame(() => requestAnimationFrame(() => {
        const el = document.getElementById(sel);
        if (el) el.scrollTop = el.scrollHeight;
      }));
    },
    scroll_follow: (id) => {
      const sel = str(id);
      const el = document.getElementById(sel);
      // At the bottom within a few pixels (zoom and fractional heights).
      if (!el || el.scrollHeight - el.scrollTop - el.clientHeight > 8) return;
      const top = el.scrollTop;
      requestAnimationFrame(() => requestAnimationFrame(() => {
        const now = document.getElementById(sel);
        if (now && now.scrollTop >= top) now.scrollTop = now.scrollHeight;
      }));
    },
  };
}
