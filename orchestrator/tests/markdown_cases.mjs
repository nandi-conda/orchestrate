// Runs ui/markdown.js on each case and prints [{name, got, want}] as JSON for
// test_markdown.py. The ops are rendered to HTML the way app.py's md() replays
// them into ultra, so a case reads as the markup the window ends up with.
import { parse, TAGS, END, TEXT, HREF, START, ALIGN_C, ALIGN_R, SRC, ALT } from "../ui/markdown.js";

const esc = (s) => s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

export function html(src) {
  const { op, s } = parse(src);
  let out = "";
  const stack = [];
  let pending = null; // the open tag, until its first child or attribute is done
  const close = () => { if (pending !== null) { out += pending + ">"; pending = null; } };
  for (let i = 0; i < op.length; i++) {
    const o = op[i];
    if (o === HREF) { pending += ` href="${s[i]}"`; continue; }
    if (o === SRC) { pending += ` src="${s[i]}"`; continue; }
    if (o === ALT) { pending += ` alt="${esc(s[i])}"`; continue; }
    if (o === START) { pending += ` start="${s[i]}"`; continue; }
    if (o === ALIGN_C || o === ALIGN_R) { pending += ` class="${o === ALIGN_C ? "center" : "right"}"`; continue; }
    close();
    if (o === TEXT) { out += esc(s[i]); continue; }
    if (o === END) {
      const t = stack.pop();
      if (t !== "br" && t !== "hr" && t !== "img") out += `</${t}>`;
      continue;
    }
    const [tag, c] = TAGS[o].split(".");
    stack.push(tag);
    pending = `<${tag}` + (c ? ` class="${c}"` : "");
  }
  close();
  return out;
}

const cases = [
  ["paragraph and line break", "one\ntwo\n\nthree", "<p>one<br>two</p><p>three</p>"],
  ["headings", "# Title\n### Sub ###", "<h1>Title</h1><h3>Sub</h3>"],
  ["emphasis", "**bold** *it* _it_ ~~gone~~ ***both***",
   "<p><strong>bold</strong> <em>it</em> <em>it</em> <del>gone</del> <strong><em>both</em></strong></p>"],
  ["intraword underscore stays", "snake_case_name", "<p>snake_case_name</p>"],
  ["inline code keeps markdown literal", "run `a **b** <c>` now", "<p>run <code>a **b** &lt;c&gt;</code> now</p>"],
  ["escapes", "\\*not\\* italic", "<p>*not* italic</p>"],
  ["links", "see [docs](https://x.dev/a_(b)) and <https://y.dev>",
   '<p>see <a href="https://x.dev/a_(b)">docs</a> and <a href="https://y.dev">https://y.dev</a></p>'],
  ["bare url without trailing period", "at https://z.dev/p.", '<p>at <a href="https://z.dev/p">https://z.dev/p</a>.</p>'],
  ["javascript links are not linked", "[x](javascript:alert(1))", "<p>x</p>"],
  ["remote images show as links", "![logo](https://x.dev/l.png)", '<p><a href="https://x.dev/l.png">logo</a></p>'],
  ["worktree images show", "see ![the **plot**](out/plot.png) and ![](/docs/a.svg)",
   '<p>see <img src="out/plot.png" alt="the plot"> and <img src="/docs/a.svg" alt=""></p>'],
  ["a linked worktree image", "[![x](../x.png)](https://x.dev)",
   '<p><a href="https://x.dev"><img src="../x.png" alt="x"></a></p>'],
  ["scheme images are not fetched", "![a](javascript:x) ![b](data:image/png;base64,AA) ![c](//evil.dev/c.png)",
   '<p>a b <a href="//evil.dev/c.png">c</a></p>'],
  ["bullet list", "- a\n- b\n  - c\n- d",
   "<ul><li>a</li><li>b<ul><li>c</li></ul></li><li>d</li></ul>"],
  ["ordered list with start", "3. three\n4. four", '<ol start="3"><li>three</li><li>four</li></ol>'],
  ["sub-list under a number, indented by two", "1. one\n  - sub\n2. two",
   "<ol><li>one<ul><li>sub</li></ul></li><li>two</li></ol>"],
  ["loose list", "- a\n\n- b", "<ul><li><p>a</p></li><li><p>b</p></li></ul>"],
  ["list item with code", "- run:\n  ```sh\n  ls\n  ```",
   '<ul><li>run:<div class="md-code"><div class="md-lang">sh</div><pre><code>ls</code></pre></div></li></ul>'],
  ["code fence", "```py\ndef f():\n    return 1\n```\nafter",
   '<div class="md-code"><div class="md-lang">py</div><pre><code>def f():\n    return 1</code></pre></div><p>after</p>'],
  ["unclosed fence while streaming", "text\n```js\nconst a = 1;",
   '<p>text</p><div class="md-code"><div class="md-lang">js</div><pre><code>const a = 1;</code></pre></div>'],
  ["unclosed bold while streaming stays literal", "this is **bol", "<p>this is **bol</p>"],
  ["blockquote", "> quoted\n> **more**", "<blockquote><p>quoted<br><strong>more</strong></p></blockquote>"],
  ["rule", "a\n\n---\n\nb", "<p>a</p><hr><p>b</p>"],
  ["table", "| a | b |\n|:-:|--:|\n| `x|y` | 2 |",
   '<div class="md-table"><table><thead><tr><th class="center">a</th><th class="right">b</th></tr></thead>' +
   '<tbody><tr><td class="center"><code>x|y</code></td><td class="right">2</td></tr></tbody></table></div>'],
  ["table head before its delimiter has streamed", "| a | b |", "<p>| a | b |</p>"],
  ["html is text", "<script>x</script>", "<p>&lt;script&gt;x&lt;/script&gt;</p>"],
  ["heading interrupts paragraph", "intro\n## Next", "<p>intro</p><h2>Next</h2>"],
];

console.log(JSON.stringify(cases.map(([name, src, want]) => ({ name, got: html(src), want }))));
