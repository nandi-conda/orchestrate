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
def o_images(o: i32, k: str) -> None:
    """Set key k of object o to the images attached to the request being composed."""
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
def j_when(e: i32, k: str, dst: i32, full: i32) -> i32:
    """Write the time in key k of JSON object e (seconds since the epoch) to
    dst in the viewer's local time zone, and the full date, time and zone to
    full. 0 when e has no such time."""
    pass

@jscall
def is_md(h: i32) -> i32:
    """1 if the file name in h is markdown (ends in .md or .markdown, any case)."""
    pass

@jscall
def is_img(h: i32) -> i32:
    """1 if the file name in h is an image the window shows (png, jpg, jpeg,
    gif, webp or svg, any case)."""
    pass

@jscall
def path_join(dst: i32, base: i32, rel: i32) -> None:
    """dst = worktree path rel, from directory base; a rel starting with "/"
    starts at the worktree's root. ".." is left for the worker to resolve."""
    pass

@jscall
def path_dir(dst: i32, path: i32) -> None:
    """dst = the directory of worktree path path ("" for the root)."""
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
    """Scroll the element with this id to the bottom after the next render,
    and follow it there (scroll_follow)."""
    pass

@jscall
def scroll_follow(id: str) -> None:
    """Like scroll_end, unless the reader scrolled the element away from the
    bottom since (until they scroll back to it)."""
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

@jscall
def on_paste(msg: i32) -> None:
    """Follow image pastes: an image on the clipboard (a screenshot, a copied
    picture) is added to the request being composed and update(msg, 1) runs.
    Pasting text is left to the input."""
    pass

@jscall
def img_count() -> i32:
    """How many images are attached to the request being composed."""
    pass

@jscall
def img_url(i: i32, dst: i32) -> None:
    """Write attached image i's data: URL to dst."""
    pass

@jscall
def img_drop(i: i32) -> None:
    pass

@jscall
def img_stash() -> None:
    """Set the attached images aside while their request is in flight."""
    pass

@jscall
def img_restore() -> None:
    """Put the images set aside by img_stash back (the send failed)."""
    pass

@jscall
def img_clear() -> None:
    pass

@jscall
def on_zoom() -> None:
    """Clicking an image with the `zoom` class shows it enlarged, and again
    puts it back."""
    pass

@jscall
def j_put_row(rows: i32, row: i32) -> i32:
    """Put JSON object row into the JSON array rows, by its `seq`: 1 when it
    replaced the row already there, 0 when it started a new one."""
    pass

@jscall
def host_folders() -> i32:
    """1 when the page is in the desktop window, whose Browse… is the OS's
    folder dialog (host_pick_folder); 0 in a browser, which has the UI's own
    folder picker."""
    pass

@jscall
def host_pick_folder(h: i32, msg: i32, arg: i32) -> None:
    """Show the desktop window's folder dialog, open at the folder in h. When
    a folder is chosen it goes to h and update(msg, arg) runs; a cancel does
    nothing."""
    pass
