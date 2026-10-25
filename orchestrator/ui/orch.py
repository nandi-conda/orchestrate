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
