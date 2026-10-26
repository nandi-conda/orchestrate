// Runs ui/imports.js under node: the pane's rows (j_put_row) take a live row by
// its seq, so a reply still streaming grows one row instead of making the pane
// fetch every event again. Prints [{name, got, want}] for test_ui_imports.py.
const noop = () => {};
globalThis.document = { addEventListener: noop, getElementById: () => null };
const { imports } = await import("../ui/imports.js");

const strs = [], vals = [undefined];
const api = imports({ str: (h) => strs[h] ?? "", strs, vals, dispatch: noop });
const put = (v) => { vals.push(v); return vals.length - 1; };
const texts = (h) => vals[h].map((r) => r.text);

const cases = [];
const case_ = (name, want, run) => cases.push({ name, want, got: run() });

case_("a part that starts is a new row", { grew: 0, rows: ["first", "second"] }, () => {
  const rows = put([{ seq: 1, text: "first" }]);
  const grew = api.j_put_row(rows, put({ seq: 2, text: "second" }));
  return { grew, rows: texts(rows) };
});

case_("a reply still streaming grows its own row", { grew: 1, rows: ["first two", "second"] }, () => {
  const rows = put([{ seq: 1, text: "first" }, { seq: 2, text: "second" }]);
  const grew = api.j_put_row(rows, put({ seq: 1, text: "first two" }));
  return { grew, rows: texts(rows) };
});

case_("the row is the pane's own, so the event can be freed", { rows: ["kept"] }, () => {
  const rows = put([{ seq: 1, text: "kept" }]);
  const row = put({ seq: 1, text: "kept" });
  api.j_put_row(rows, row);
  vals[row] = undefined; // j_free
  return { rows: texts(rows) };
});

case_("nothing to patch yet is not a row that grew", { grew: 0, blank: 0 }, () => {
  const rows = put([]);
  return { grew: api.j_put_row(0, put({ seq: 1, text: "x" })) + api.j_put_row(rows, 0),
           blank: vals[rows].length };
});

case_("a resumed stream does not shrink the row the pane has", { grew: 1, rows: ["first two"] }, () => {
  const rows = put([{ seq: 1, text: "first two" }]);
  const grew = api.j_put_row(rows, put({ seq: 1, text: "two" }));
  return { grew, rows: texts(rows) };
});

console.log(JSON.stringify(cases));
