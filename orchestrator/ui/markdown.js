// Markdown for agent replies, turned into a flat list of render ops that
// app.py's md() replays through ultra's el/text/end. The parse runs on every
// render while a reply streams in, so it is pure, linear and memoised.
//
// A reply cut off mid-stream must still read well: an unclosed code fence
// runs to the end of the text, and an unclosed **, ` or [ stays literal
// until its closer arrives.
//
// ops: END closes the open element, TEXT adds text, HREF / START set the open
// element's href / start, ALIGN_* add a column alignment class, SRC / ALT set
// an img's worktree path and alt text, and any code in TAGS opens that element
// ("tag.class"). app.py's md_open() mirrors TAGS.

export const END = 0, TEXT = 1, HREF = 2, START = 3, ALIGN_C = 4, ALIGN_R = 5, SRC = 6, ALT = 7;
export const TAGS = {
  10: "p", 11: "h1", 12: "h2", 13: "h3", 14: "h4", 15: "h5", 16: "h6",
  17: "ul", 18: "ol", 19: "li", 20: "pre", 21: "code", 22: "blockquote", 23: "hr",
  24: "table", 25: "thead", 26: "tbody", 27: "tr", 28: "th", 29: "td",
  30: "strong", 31: "em", 32: "del", 33: "a", 34: "br",
  35: "div.md-code", 36: "div.md-lang", 37: "div.md-table", 38: "img",
};
const T = Object.fromEntries(Object.entries(TAGS).map(([k, v]) => [v, +k]));

class Out {
  constructor() { this.op = []; this.s = []; }
  open(tag) { this.op.push(T[tag]); this.s.push(""); }
  end() { this.op.push(END); this.s.push(""); }
  set(op, s) { this.op.push(op); this.s.push(s); }
  text(s) {
    if (!s) return;
    const n = this.op.length - 1;
    if (n >= 0 && this.op[n] === TEXT) this.s[n] += s; // one text node per run
    else this.set(TEXT, s);
  }
}

// ------------------------------------------------------------ blocks

const FENCE = /^ {0,3}(`{3,}|~{3,})\s*([^`\s]*)/;
const HEADING = /^ {0,3}(#{1,6})(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$/;
const RULE = /^ {0,3}([-*_])(?:[ \t]*\1){2,}[ \t]*$/;
const QUOTE = /^ {0,3}> ?/;
const ITEM = /^( {0,3})([-*+]|(\d{1,9})[.)])([ \t]+|$)/;
const DELIM = /^ {0,3}\|?[ \t]*:?-+:?[ \t]*(\|[ \t]*:?-+:?[ \t]*)*\|?[ \t]*$/;
const blank = (l) => l.trim() === "";

function cells(line) {
  let s = line.trim();
  if (s.startsWith("|")) s = s.slice(1);
  if (s.endsWith("|") && !s.endsWith("\\|")) s = s.slice(0, -1);
  const out = [];
  let cur = "", code = false;
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    if (c === "\\" && s[i + 1] === "|") { cur += "|"; i++; continue; }
    if (c === "`") code = !code;
    if (c === "|" && !code) { out.push(cur.trim()); cur = ""; continue; }
    cur += c;
  }
  out.push(cur.trim());
  return out;
}

// Does a line start a block other than a paragraph? (It ends a paragraph.)
function interrupts(l) {
  return FENCE.test(l) || HEADING.test(l) || RULE.test(l) || QUOTE.test(l) || ITEM.test(l);
}

function blocks(lines, out, tightItem = false) {
  let i = 0;
  const n = lines.length;
  while (i < n) {
    const l = lines[i];
    if (blank(l)) { i++; continue; }
    let m;
    if ((m = FENCE.exec(l))) {
      const fence = m[1];
      const close = new RegExp("^ {0,3}" + fence[0] + "{" + fence.length + ",}\\s*$");
      const body = [];
      i++;
      while (i < n && !close.test(lines[i])) body.push(lines[i++]);
      i++; // the closing fence, or past the end while it is still streaming
      out.open("div.md-code");
      if (m[2]) { out.open("div.md-lang"); out.text(m[2]); out.end(); }
      out.open("pre"); out.open("code"); out.text(body.join("\n")); out.end(); out.end();
      out.end();
      continue;
    }
    if ((m = HEADING.exec(l))) {
      out.open("h" + m[1].length);
      inline(m[2] || "", out);
      out.end();
      i++;
      continue;
    }
    if (RULE.test(l)) { out.open("hr"); out.end(); i++; continue; }
    if (QUOTE.test(l)) {
      const body = [];
      while (i < n && !blank(lines[i])) {
        // a quote runs on over lazy continuation lines, until a blank line
        body.push(lines[i].replace(QUOTE, ""));
        i++;
      }
      out.open("blockquote");
      blocks(body, out);
      out.end();
      continue;
    }
    if ((m = ITEM.exec(l))) { i = list(lines, i, out); continue; }
    if (l.includes("|") && i + 1 < n && DELIM.test(lines[i + 1]) && lines[i + 1].includes("-")) {
      i = table(lines, i, out);
      continue;
    }
    // paragraph: runs until a blank line or the start of another block
    const para = [l.trim()];
    i++;
    while (i < n && !blank(lines[i]) && !interrupts(lines[i])) para.push(lines[i++].trim());
    if (!tightItem) out.open("p");
    para.forEach((p, k) => {
      if (k > 0) { out.open("br"); out.end(); }
      inline(p, out);
    });
    if (!tightItem) out.end();
  }
}

function list(lines, i, out) {
  const first = ITEM.exec(lines[i]);
  const ordered = first[3] !== undefined;
  const marker = ordered ? first[2].slice(-1) : first[2];
  out.open(ordered ? "ol" : "ul");
  if (ordered && first[3] !== "1") out.set(START, String(+first[3]));
  const items = [];
  let loose = false;
  const n = lines.length;
  while (i < n) {
    const m = ITEM.exec(lines[i]);
    if (!m || (m[3] !== undefined) !== ordered || (ordered ? m[2].slice(-1) : m[2]) !== marker) break;
    const indent = m[0].length - (m[4] === "" ? 0 : m[4].length - 1);
    const body = [lines[i].slice(m[0].length)];
    i++;
    let gap = false;
    while (i < n) {
      const l = lines[i];
      if (blank(l)) { body.push(""); gap = true; i++; continue; }
      const lead = l.length - l.trimStart().length;
      // deeper than the content, or a sub-list marker indented past ours
      if (lead >= indent || (lead > m[1].length && ITEM.test(l))) {
        body.push(l.slice(Math.min(lead, indent)));
        if (gap) loose = true;
        gap = false;
        i++;
        continue;
      }
      if (gap || ITEM.test(l) || interrupts(l)) break;
      body.push(l.trim()); // lazy continuation of the item's paragraph
      i++;
    }
    while (body.length && blank(body[body.length - 1])) body.pop();
    items.push(body);
    if (gap && i < n && ITEM.test(lines[i])) loose = true;
    if (gap) {
      // a blank line, then something that is not the next item, ends the list
      const nx = i < n ? ITEM.exec(lines[i]) : null;
      if (!nx) break;
    }
  }
  for (const body of items) {
    out.open("li");
    blocks(body, out, !loose);
    out.end();
  }
  out.end();
  return i;
}

function table(lines, i, out) {
  const head = cells(lines[i]);
  const align = cells(lines[i + 1]).map((c) =>
    c.startsWith(":") && c.endsWith(":") ? ALIGN_C : c.endsWith(":") ? ALIGN_R : 0);
  const row = (cs, tag) => {
    out.open("tr");
    for (let k = 0; k < head.length; k++) {
      out.open(tag);
      if (align[k]) out.set(align[k], "");
      inline(cs[k] ?? "", out);
      out.end();
    }
    out.end();
  };
  out.open("div.md-table");
  out.open("table");
  out.open("thead"); row(head, "th"); out.end();
  i += 2;
  const n = lines.length;
  const body = [];
  while (i < n && !blank(lines[i]) && lines[i].includes("|") && !interrupts(lines[i])) body.push(cells(lines[i++]));
  if (body.length) {
    out.open("tbody");
    for (const cs of body) row(cs, "td");
    out.end();
  }
  out.end();
  out.end();
  return i;
}

// ------------------------------------------------------------ inline

const PUNCT = /[!-/:-@[-`{-~]/;
const URL_OK = /^(https?:|mailto:|\/|#)/i;
// A path in the thread's worktree: no scheme, not //host, not #anchor.
const WORKTREE = /^(?![a-z][a-z0-9+.-]*:|\/\/|#)./i;
const BARE = /^https?:\/\/[^\s<]*[^\s<.,:;"')\]!?*_~]/;
const space = (c) => c === undefined || /\s/.test(c);
const word = (c) => c !== undefined && /[\p{L}\p{N}]/u.test(c);

function inline(s, out) {
  let i = 0, buf = "";
  const flush = () => { out.text(buf); buf = ""; };
  const n = s.length;
  while (i < n) {
    const c = s[i];
    if (c === "\\" && PUNCT.test(s[i + 1] || "")) { buf += s[i + 1]; i += 2; continue; }
    if (c === "`") {
      let k = i;
      while (s[k] === "`") k++;
      const ticks = s.slice(i, k);
      const close = s.indexOf(ticks, k);
      if (close < 0 || s[close + ticks.length] === "`") { buf += ticks; i = k; continue; }
      let code = s.slice(k, close);
      if (code.length > 2 && code[0] === " " && code[code.length - 1] === " " && code.trim()) code = code.slice(1, -1);
      flush();
      out.open("code"); out.text(code); out.end();
      i = close + ticks.length;
      continue;
    }
    if (c === "!" && s[i + 1] === "[") {
      // An image in the thread's worktree is shown; a remote one is a link:
      // replies should not pull in remote content.
      const l = link(s, i + 1);
      if (l) { flush(); if (WORKTREE.test(l.url)) emitImage(l, out); else emitLink(l, out); i = l.next; continue; }
    }
    if (c === "[") {
      const l = link(s, i);
      if (l) { flush(); emitLink(l, out); i = l.next; continue; }
    }
    if (c === "<") {
      const m = /^<((?:https?:\/\/|mailto:)[^\s<>]+)>/i.exec(s.slice(i));
      if (m) { flush(); emitLink({ text: null, url: m[1] }, out); i += m[0].length; continue; }
    }
    if (c === "h" && !word(s[i - 1])) {
      const m = BARE.exec(s.slice(i));
      if (m) { flush(); emitLink({ text: null, url: m[0] }, out); i += m[0].length; continue; }
    }
    if (c === "*" || c === "_" || c === "~") {
      const e = emphasis(s, i);
      if (e) {
        flush();
        out.open(e.tag);
        inline(s.slice(e.start, e.end), out);
        out.end();
        i = e.next;
        continue;
      }
      let k = i;
      while (s[k] === c) k++;
      buf += s.slice(i, k);
      i = k;
      continue;
    }
    buf += c;
    i++;
  }
  flush();
}

// [text](url) at s[i] === "[", or null.
function link(s, i) {
  let depth = 0, k = i;
  for (; k < s.length; k++) {
    if (s[k] === "\\") { k++; continue; }
    if (s[k] === "`") { const c = s.indexOf("`", k + 1); if (c < 0) return null; k = c; continue; }
    if (s[k] === "[") depth++;
    else if (s[k] === "]" && --depth === 0) break;
  }
  if (k >= s.length || s[k + 1] !== "(") return null;
  const m = /^\(\s*<?([^\s()<>]*(?:\([^\s()]*\)[^\s()<>]*)*)>?(?:\s+(?:"[^"]*"|'[^']*'))?\s*\)/.exec(s.slice(k + 1));
  if (!m) return null;
  return { text: s.slice(i + 1, k), url: m[1], next: k + 1 + m[0].length };
}

function emitLink(l, out) {
  if (!URL_OK.test(l.url)) {
    // javascript: and friends are shown, never linked
    if (l.text === null) out.text(l.url); else inline(l.text, out);
    return;
  }
  out.open("a");
  out.set(HREF, l.url);
  if (l.text === null) out.text(l.url); else inline(l.text, out);
  out.end();
}

// The alt text is the image's text with its markup dropped.
function emitImage(l, out) {
  const alt = new Out();
  inline(l.text, alt);
  out.open("img");
  out.set(SRC, l.url);
  out.set(ALT, alt.op.map((o, k) => (o === TEXT ? alt.s[k] : "")).join(""));
  out.end();
}

// A run of * / _ / ~ at s[i] that opens emphasis with a matching closer.
function emphasis(s, i) {
  const c = s[i];
  let k = i;
  while (s[k] === c) k++;
  const run = k - i;
  if (c === "~" && run !== 2) return null;
  if (space(s[k])) return null; // an opener is followed by non-space
  if (c === "_" && word(s[i - 1])) return null; // no intraword _
  const sizes = c === "~" ? [2] : run >= 3 ? [3, 2, 1] : [run];
  for (const size of sizes) {
    const delim = c.repeat(size);
    for (let j = i + size; j < s.length; j++) {
      if (s[j] === "\\") { j++; continue; }
      if (s[j] === "`") { const e = s.indexOf("`", j + 1); if (e > 0) { j = e; continue; } }
      if (s[j] !== c) continue;
      let r = j;
      while (s[r] === c) r++;
      const closeRun = r - j;
      if (closeRun >= size && !space(s[j - 1]) && j > i + size && !(c === "_" && word(s[r]))) {
        // A longer run matches its first `size` chars when the run is exactly ours
        // or the rest belongs to an outer delimiter.
        const at = closeRun === size ? j : r - size;
        // *** is <strong> around <em>: the inner *…* is parsed again
        if (size === 3) return { tag: "strong", start: i + 2, end: at + 1, next: at + 3 };
        return { tag: c === "~" ? "del" : size === 2 ? "strong" : "em", start: i + size, end: at, next: at + size };
      }
      j = r - 1;
    }
  }
  return null;
}

// ------------------------------------------------------------ entry

const memo = new Map();

export function parse(src) {
  let d = memo.get(src);
  if (d) return d;
  const out = new Out();
  blocks(src.replace(/\r\n?/g, "\n").split("\n"), out);
  d = { op: out.op, s: out.s };
  if (memo.size >= 64) memo.delete(memo.keys().next().value);
  memo.set(src, d);
  return d;
}
