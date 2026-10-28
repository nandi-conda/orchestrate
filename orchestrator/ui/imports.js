// Extra imports for ui/orch.py, passed to ultra's mount(..., { imports }).
import { parse } from "./markdown.js";

export function imports({ str, strs, vals, dispatch }) {
  const put = (v) => { vals.push(v); return vals.length - 1; };
  const streams = new Map();
  let nextStream = 1;
  let doc = { op: [], s: [] }; // the markdown md_parse last parsed
  const mdCache = new Map(); // markdown source -> its parsed doc
  const MD_CACHE = 4096;
  const codeIds = new Map(), codeText = [""]; // code_put: place -> id, id -> text
  const follow = new Map(); // element id -> keeps the bottom in view (scroll_follow)
  const placed = new WeakMap(); // element -> the scrollTop toEnd last set
  const toEnd = (sel) => {
    const el = document.getElementById(sel);
    if (!el) return;
    el.scrollTop = el.scrollHeight;
    placed.set(el, el.scrollTop);
  };
  document.addEventListener("scroll", (e) => {
    const el = e.target;
    if (!el.id || !follow.has(el.id) || placed.get(el) === el.scrollTop) return;
    // At the bottom within a few pixels (zoom and fractional heights).
    follow.set(el.id, el.scrollHeight - el.scrollTop - el.clientHeight <= 8);
  }, true);

  // Images pasted into the request box. They stay data: URLs here until the
  // request is sent; `stashed` holds them while a send is in flight, so a
  // refusal can put them back. The server stores them and events carry ids.
  const MAX_IMAGES = 4, MAX_DIM = 1800, SHRINK_OVER = 1500000;
  let images = [], stashed = [];
  const readDataURL = (f) => new Promise((resolve, reject) => {
    const r = new FileReader();
    r.onload = () => resolve(String(r.result));
    r.onerror = () => reject(r.error);
    r.readAsDataURL(f);
  });
  // A huge paste is scaled down before it goes anywhere: screenshots of a 4K
  // screen are far more pixels than a model needs. Small ones keep their bytes.
  const shrink = async (f) => {
    if (f.size <= SHRINK_OVER) return readDataURL(f);
    const url = URL.createObjectURL(f);
    try {
      const img = await new Promise((resolve, reject) => {
        const i = new Image();
        i.onload = () => resolve(i);
        i.onerror = reject;
        i.src = url;
      });
      const scale = Math.min(1, MAX_DIM / Math.max(img.naturalWidth, img.naturalHeight));
      if (scale === 1) return readDataURL(f);
      const c = document.createElement("canvas");
      c.width = Math.round(img.naturalWidth * scale);
      c.height = Math.round(img.naturalHeight * scale);
      c.getContext("2d").drawImage(img, 0, 0, c.width, c.height);
      return c.toDataURL(f.type === "image/png" ? "image/png" : "image/jpeg", 0.9);
    } finally {
      URL.revokeObjectURL(url);
    }
  };
  return {
    o_new: () => put({}),
    o_str: (o, k, h) => { vals[o][str(k)] = strs[h] ?? ""; },
    o_lit: (o, k, v) => { vals[o][str(k)] = str(v); },
    o_int: (o, k, h) => { vals[o][str(k)] = parseInt(strs[h], 10); },
    o_images: (o, k) => { if (images.length) vals[o][str(k)] = images.slice(); },
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
                       "saved", "landed", "land_conflict", "preparing", "prepared",
                       "archived", "unarchived", "edited", "objectives"])
        es.addEventListener(t, es.onmessage);
      streams.set(id, es);
      return id;
    },
    sse_close: (id) => { const es = streams.get(id); if (es) { es.close(); streams.delete(id); } },
    s_add_enc: (d, h) => { strs[d] = (strs[d] ?? "") + encodeURIComponent(strs[h] ?? ""); },
    // An event's time (its `created_at`, seconds since the epoch) in the
    // viewer's own time zone: the time of day, with the date when it is not
    // today; the full date, time and zone in `full` for the hover title.
    j_when: (e, k, d, full) => {
      const t = Number(vals[e]?.[str(k)]);
      if (!Number.isFinite(t) || t <= 0) return 0;
      const at = new Date(t * 1000);
      const hm = { hour: "2-digit", minute: "2-digit" };
      strs[d] = at.toDateString() === new Date().toDateString()
        ? at.toLocaleTimeString([], hm)
        : at.toLocaleString([], { month: "short", day: "numeric", ...hm });
      strs[full] = at.toLocaleString([], { dateStyle: "full", timeStyle: "long" });
      return 1;
    },
    is_md: (h) => /\.(md|markdown)$/i.test(strs[h] ?? "") ? 1 : 0,
    is_img: (h) => /\.(png|jpe?g|gif|webp|svg)$/i.test(strs[h] ?? "") ? 1 : 0,
    path_join: (d, b, r) => {
      const rel = strs[r] ?? "", base = strs[b] ?? "";
      strs[d] = rel.startsWith("/") ? rel.replace(/^\/+/, "") : base ? base + "/" + rel : rel;
    },
    path_dir: (d, h) => { const p = strs[h] ?? ""; strs[d] = p.includes("/") ? p.slice(0, p.lastIndexOf("/")) : ""; },
    page_project: (d) => {
      const m = location.pathname.match(/^\/p\/([^/]+)/);
      if (!m) return 0;
      strs[d] = decodeURIComponent(m[1]);
      return 1;
    },
    nav_push: (u) => { history.pushState(null, "", strs[u]); },
    host_keys: () => (window.orchestrateApp ? 1 : 0),
    open_host_keys: () => { window.orchestrateApp.keys(); },
    on_nav: (msg) => { addEventListener("popstate", () => dispatch(msg, 0)); },
    set_title: (h) => { document.title = strs[h]; },
    // Parsed markdown, by its source: a pane with a long history re-renders every
    // reply on each frame, and only the one still streaming changes. The cache
    // is dropped whole rather than grown without bound: a session's tail is all
    // a pane can show anyway.
    md_parse: (h) => {
      const src = strs[h] ?? "";
      let d = mdCache.get(src);
      if (d === undefined) {
        if (mdCache.size >= MD_CACHE) mdCache.clear();
        d = parse(src);
        mdCache.set(src, d);
      }
      doc = d;
      return doc.op.length;
    },
    md_op: (i) => doc.op[i],
    md_str: (i, d) => { strs[d] = doc.s[i]; },
    // Code the panes show, for its copy click: an id per place (key), which
    // keeps it while the text there grows. Ids start at 1 (0 = none).
    code_put: (k, t) => {
      const key = strs[k] ?? "";
      let id = codeIds.get(key);
      if (id === undefined) { id = codeText.length; codeIds.set(key, id); codeText.push(""); }
      codeText[id] = strs[t] ?? "";
      return id;
    },
    code_text: (id, d) => { strs[d] = codeText[id] ?? ""; },
    // A pane row by seq: a live row (a reply still streaming) is one seq that
    // grows, and a part that starts is a new row at the end of the list. A row
    // never shrinks: a stream resumed mid-reply after a reconnect starts its
    // count again, and the row the pane has is the fuller one.
    j_put_row: (rows, row) => {
      const list = vals[rows], r = vals[row];
      if (!Array.isArray(list) || !r) return 0;
      const at = list.findIndex((x) => x && x.seq === r.seq);
      if (at < 0) { list.push({ ...r }); return 0; }
      const had = list[at];
      if (String(r.text ?? "").length < String(had.text ?? "").length) return 1;
      list[at] = { ...r };
      return 1;
    },
    // Copy: one path, the async Clipboard API. A click is a user gesture and
    // the page is served from localhost (a secure context), in a browser and
    // in the desktop window alike.
    clip_copy: (h, ok, arg, fail) => {
      navigator.clipboard.writeText(strs[h] ?? "").then(
        () => dispatch(ok, arg),
        (e) => { strs[h] = String(e && e.message || e); dispatch(fail, h); });
    },
    // Text selected inside the pane (#pane): its text goes to th, a style
    // that puts the Reply button over it to sh, the seq of the event row it
    // starts in to qh ("" for a row still streaming, whose seq won't last),
    // and update(msg, 1). When the selection goes away (or leaves the pane),
    // update(msg, 0).
    on_select: (th, sh, qh, msg) => {
      let last = "", queued = false;
      const inPane = (n) => !!(n && (n.nodeType === 1 ? n : n.parentElement)?.closest("#pane"));
      const check = () => {
        queued = false;
        const sel = getSelection();
        let txt = "", style = "", src = "";
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
            const n = r.startContainer;
            const row = (n.nodeType === 1 ? n : n.parentElement).closest(".ev[id^='ev-']");
            if (row && !row.classList.contains("ev-live")) src = row.id.slice(3);
          }
        }
        const k = txt ? txt + "\u0000" + style + "\u0000" + src : "";
        if (k === last) return;
        last = k;
        strs[th] = txt;
        strs[sh] = style;
        strs[qh] = src;
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
    // Bring the pane's row for event seq into view after the next render and
    // flash it; update(miss, seq) if the pane doesn't have it. Scrolling it
    // into view leaves the bottom, so the pane stops following new events
    // until the reader scrolls back down.
    jump_to: (seq, miss) => {
      requestAnimationFrame(() => requestAnimationFrame(() => {
        const row = document.getElementById("ev-" + seq);
        if (!row || !row.closest("#pane")) { dispatch(miss, seq); return; }
        row.scrollIntoView({ block: "center", behavior: "smooth" });
        row.classList.remove("ev-flash");
        void row.offsetWidth; // restart the flash when it is already on
        row.classList.add("ev-flash");
        row.addEventListener("animationend", () => row.classList.remove("ev-flash"), { once: true });
      }));
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
    // Pasting: an image on the clipboard becomes an attachment of the request
    // being composed. Text pastes are left to the input.
    on_paste: (msg) => {
      document.addEventListener("paste", (e) => {
        if (!document.getElementById("req")) return; // no composer on this screen
        const items = [...(e.clipboardData?.items || [])]
          .filter((it) => it.kind === "file" && it.type.startsWith("image/"));
        const files = items.map((it) => it.getAsFile()).filter(Boolean);
        if (!files.length) return;
        e.preventDefault();
        (async () => {
          for (const f of files) {
            if (images.length >= MAX_IMAGES) break;
            try { images.push(await shrink(f)); dispatch(msg, images.length); } catch {}
          }
        })();
      }, true);
    },
    // The request box is a textarea: Shift+Enter is kept from ultra's Enter
    // handler (on the root, below document) so the browser adds the line,
    // and plain Enter, which sends, adds none.
    newline_keys: () => {
      document.addEventListener("keydown", (e) => {
        if (e.key !== "Enter" || e.isComposing || e.target.id !== "req") return;
        if (e.shiftKey) e.stopPropagation();
        else e.preventDefault();
      }, true);
    },
    img_count: () => images.length,
    img_url: (i, d) => { strs[d] = images[i] ?? ""; },
    img_drop: (i) => { images.splice(i, 1); },
    img_stash: () => { stashed = images; images = []; },
    img_restore: () => { images = stashed.concat(images); stashed = []; },
    img_clear: () => { images = []; stashed = []; },
    // An image with the `zoom` class grows to fill the window; click it again
    // (or another one) to go back.
    on_zoom: () => {
      document.addEventListener("click", (e) => {
        const img = e.target.closest && e.target.closest("img.zoom");
        if (!img) return;
        const on = img.classList.contains("zoomed");
        document.querySelectorAll("img.zoom.zoomed").forEach((n) => n.classList.remove("zoomed"));
        if (!on) img.classList.add("zoomed");
      });
    },
    // Scroll following. An element follows its new content until the reader
    // scrolls it away from the bottom, and again once they scroll back. Only
    // a change of scrollTop fires "scroll": the reader, our own scrolls
    // (skipped), or the browser clamping a shrunk element, which leaves it
    // at the bottom. Content growing or the element resizing fires none, so
    // they never turn following off.
    // The desktop window's folder dialog (desktop/host.js). A browser has no
    // host, and the UI shows its own folder picker there instead.
    host_folders: () => (window.orchestrateHost ? 1 : 0),
    host_pick_folder: (h, msg, arg) => {
      window.orchestrateHost.pickFolder(strs[h] ?? "").then((path) => {
        if (!path) return;
        strs[h] = path;
        dispatch(msg, arg);
      });
    },
    scroll_end: (id) => {
      const sel = str(id);
      follow.set(sel, true);
      requestAnimationFrame(() => requestAnimationFrame(() => toEnd(sel)));
    },
    scroll_top: (id) => {
      const sel = str(id);
      follow.set(sel, false);
      requestAnimationFrame(() => requestAnimationFrame(() => {
        const el = document.getElementById(sel);
        if (el) el.scrollTop = 0;
      }));
    },
    scroll_follow: (id) => {
      const sel = str(id);
      requestAnimationFrame(() => requestAnimationFrame(() => { if (follow.get(sel) !== false) toEnd(sel); }));
    },
  };
}
