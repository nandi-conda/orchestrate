# Extra @jscall imports for the orchestrator UI. ui/imports.js implements them
# and app passes them to ultra's mount(..., { imports }).

from lpython import i32


@jscall
def o_new() -> i32:
    """A new empty JSON object (a JSON handle) to fill and POST."""
    pass

@jscall
def o_str(o: i32, k: str, h: i32) -> None:
    """Set key k of object o to the string in handle h."""
    pass

@jscall
def o_lit(o: i32, k: str, v: str) -> None:
    pass

@jscall
def http_post(url: i32, o: i32, msg: i32) -> i32:
    """POST object o as JSON to the URL in handle url. Like http_get: returns a
    JSON handle now, and update(msg, handle) runs when the reply arrives."""
    pass

@jscall
def sse_open(url: i32, msg: i32) -> i32:
    """Open an EventSource; update(msg, handle) for each event's JSON data."""
    pass

@jscall
def sse_close(id: i32) -> None:
    pass

@jscall
def http_get_text(url: i32, msg: i32) -> i32:
    """GET a plain-text reply. Returns a JSON handle now; update(msg, handle)
    runs when it arrives, with the body under "text" (or the error)."""
    pass

@jscall
def s_add_enc(dst: i32, src: i32) -> None:
    """Append the string in src to dst, encoded for a URL query (encodeURIComponent)."""
    pass

@jscall
def page_project(dst: i32) -> i32:
    """If the address is a project's page (/p/<id>), write the id to dst and
    return 1."""
    pass

@jscall
def nav_push(url: i32) -> None:
    """Move the address bar to url (history.pushState) without leaving the page."""
    pass

@jscall
def on_nav(msg: i32) -> None:
    """update(msg, 0) whenever Back or Forward changes the address."""
    pass

@jscall
def set_title(h: i32) -> None:
    pass

@jscall
def scroll_end(id: str) -> None:
    """Scroll the element with this id to the bottom after the next render."""
    pass

@jscall
def scroll_follow(id: str) -> None:
    """Like scroll_end, but only if the element is scrolled to the bottom now,
    before the next render, and the reader hasn't scrolled up by then."""
    pass

@jscall
def md_parse(h: i32) -> i32:
    """Parse the markdown in handle h (ui/markdown.js) and return how many
    render ops it made; md_op and md_str read them until the next md_parse."""
    pass

@jscall
def md_op(i: i32) -> i32:
    pass

@jscall
def md_str(i: i32, dst: i32) -> None:
    """Write op i's text (its text, href or start) to dst."""
    pass

@jscall
def clip_copy(h: i32, ok: i32, arg: i32, fail: i32) -> None:
    """Put the string in handle h on the clipboard. update(ok, arg) once it is
    there; if the browser refuses, h holds why and update(fail, h) runs."""
    pass

@jscall
def on_select(text: i32, style: i32, msg: i32) -> None:
    """Follow the text selection in the pane (#pane): while there is one, its
    text is in handle text, a style placing a button over it in handle style,
    and update(msg, 1) runs whenever either changes; update(msg, 0) when it ends."""
    pass

@jscall
def sel_clear() -> None:
    pass

@jscall
def s_quote(dst: i32, src: i32) -> None:
    """dst = src as a markdown block quote, "> " before each line."""
    pass

@jscall
def focus_end(id: str) -> None:
    """Focus the input with this id after the next render, caret at the end."""
    pass
