# Orchestrator UI: one window that shows the project list at / and a project at
# /p/<id>. Opening a project, going back to the list, Back and Forward all
# happen in place (history.pushState), never in a new tab or window. A project
# shows its threads on the left, the selected thread's transcript, tool calls,
# files or worker log (or the project's activity) in the middle, and a request box at
# the bottom that goes to the router.
#
# Text lives in ultra's JS string store; this module holds handles. JSON
# replies stay JS-side too, read with j_* by handle.

from lpython import i32, Const
from ultra import (el, end, text, text_ref, num, cls, cls_if, attr, attr_ref, key, autofocus,
                   on_click, on_enter, on_input, value_ref, after, s_new, s_set, s_add, s_add_ref, s_clear, s_len, s_eq,
                   s_copy, http_get_ref, j_ok, j_err, j_at, j_len, j_idx, j_str, j_int, j_has, j_free, div, span)
from ui import (btn, btn_open, card_open, card_header, card_content_open, card_footer_open,
                input_open, label, badge, dialog_open, dialog_footer_open, alert, separator,
                toggle_group_open, toggle, tabs_list_open, tab, aria)
from orch import (o_new, o_str, o_lit, http_post, http_get_text, sse_open, sse_close, page_project,
                  nav_push, on_nav, set_title, scroll_end, scroll_follow, md_parse, md_op, md_str, s_add_enc,
                  clip_copy, on_select, sel_clear, s_quote, focus_end, is_md)

# ---- Msg
GOT_PROJECTS: Const[i32] = 1
OPEN: Const[i32] = 2
NEW_SHOW: Const[i32] = 3
NEW_HIDE: Const[i32] = 4
NAME_IN: Const[i32] = 5
REPO_IN: Const[i32] = 6
CREATE: Const[i32] = 7
CREATED: Const[i32] = 8
GOT_PROJECT: Const[i32] = 9
GOT_THREADS: Const[i32] = 10
SELECT: Const[i32] = 11
GOT_TRANSCRIPT: Const[i32] = 12
REQ_IN: Const[i32] = 13
SEND: Const[i32] = 14
SENT: Const[i32] = 15
MODE: Const[i32] = 16
EVENT: Const[i32] = 17
REFRESH: Const[i32] = 18
STOP: Const[i32] = 19
STOPPED: Const[i32] = 20
HOME: Const[i32] = 21
GOT_LOG: Const[i32] = 22
ACTIVITY: Const[i32] = 23
TAB: Const[i32] = 24
NAV: Const[i32] = 25
GOT_WLOG: Const[i32] = 26
WLOG_REFRESH: Const[i32] = 27
ARCHIVE: Const[i32] = 28
UNARCHIVE: Const[i32] = 29
ARCHIVED: Const[i32] = 30
UNARCHIVED: Const[i32] = 31
SHOW_ARCHIVED: Const[i32] = 32
GOT_FILES: Const[i32] = 33
FILE_OPEN: Const[i32] = 34
FILES_UP: Const[i32] = 35
FILES_REFRESH: Const[i32] = 36
COPY: Const[i32] = 37
COPIED: Const[i32] = 38
COPY_FAILED: Const[i32] = 39
COPIED_DONE: Const[i32] = 40
SELECTED: Const[i32] = 41
QUOTE: Const[i32] = 42
UNQUOTE: Const[i32] = 43
FILE_VIEW: Const[i32] = 44
GOT_FVIEW: Const[i32] = 45
FVIEW_CLOSE: Const[i32] = 46
ARCHIVE_ROW: Const[i32] = 47
UNARCHIVE_ROW: Const[i32] = 48

# pane tabs
DIALOG: Const[i32] = 0
TOOLS: Const[i32] = 1
WLOG: Const[i32] = 2
FILES: Const[i32] = 3

# routing modes
AUTO: Const[i32] = 0
TO_THREAD: Const[i32] = 1
NEW_THREAD: Const[i32] = 2

# event kinds (control/app.py)
K_USER: Const[i32] = 1
K_ASSISTANT: Const[i32] = 2
K_TOOL_CALL: Const[i32] = 3
K_TOOL_RESULT: Const[i32] = 4
K_STATUS: Const[i32] = 5
K_PEER: Const[i32] = 6
K_ERROR: Const[i32] = 7
K_ROUTE: Const[i32] = 8
K_THINKING: Const[i32] = 9
K_RESULT: Const[i32] = 10
K_STATE: Const[i32] = 11
K_REQUEST: Const[i32] = 12

# markdown render ops (ui/markdown.js); codes from 10 open an element
MD_END: Const[i32] = 0
MD_TEXT: Const[i32] = 1
MD_HREF: Const[i32] = 2
MD_START: Const[i32] = 3
MD_ALIGN_C: Const[i32] = 4
MD_ALIGN_R: Const[i32] = 5

# ---- Model
screen: i32 = 0          # 0 project list, 1 a project
projects: i32 = 0        # JSON handle: list of projects
project: i32 = 0         # JSON handle: the open project
threads: i32 = 0         # JSON handle: thread directory
transcript: i32 = 0      # JSON handle: selected thread's events
plog: i32 = 0            # JSON handle: project activity
loading: i32 = 0
show_new: i32 = 0
creating: i32 = 0
sending: i32 = 0
mode: i32 = 0
pending: i32 = 0         # a refresh is scheduled
pane_tab: i32 = 0        # DIALOG, TOOLS, FILES or WLOG
stream: i32 = 0          # the open project's event stream, 0 when none
wlog: i32 = 0            # JSON handle: the selected thread's worker log ({text})
wlog_loading: i32 = 0
files: i32 = 0           # JSON handle: a directory of the selected thread's worktree ({path, entries})
files_loading: i32 = 0
fview: i32 = 0           # JSON handle: the markdown file the Files tab shows ({text})
fview_req: i32 = 0       # the handle its read will land in, 0 when none is pending
show_archived: i32 = 0   # the thread list shows archived threads instead of live ones
jump: i32 = 0            # the pane's next content was asked for by navigation: open it at the bottom
copied: i32 = -1         # seq of the reply just copied (shows a check), -1 when none
selecting: i32 = 0       # text is selected in the pane (the Reply button shows)

h_pid: i32 = 0           # project id
h_sel: i32 = 0           # selected thread id ("" = activity view)
h_name: i32 = 0
h_repo: i32 = 0
h_req: i32 = 0
h_last: i32 = 0          # the request being sent, put back if the send fails
h_err: i32 = 0
h_route: i32 = 0         # last router decision
h_url: i32 = 0
h_t1: i32 = 0
h_t2: i32 = 0
h_t3: i32 = 0
h_working: i32 = 0
h_idle: i32 = 0
h_failed: i32 = 0
h_queued: i32 = 0
h_stopped: i32 = 0
h_wlog_err: i32 = 0      # why the worker log could not be read
h_fpath: i32 = 0         # the worktree directory the Files tab shows ("" = its root)
h_files_err: i32 = 0     # why the files could not be read
h_fview: i32 = 0         # worktree path of the markdown file the Files tab shows ("" = the listing)
h_fview_err: i32 = 0     # why that file could not be read
h_md: i32 = 0            # scratch for markdown text
h_run: i32 = 0           # thread id of the thinking run being grouped
h_tid: i32 = 0           # scratch for a thread id while finding its title
h_clip: i32 = 0          # the text being copied
h_seltext: i32 = 0       # the text selected in the pane
h_selpos: i32 = 0        # style placing the Reply button over the selection
h_quote: i32 = 0         # the selection the next request replies to ("" = none)
h_lastq: i32 = 0         # the quote being sent, put back if the send fails
h_act: i32 = 0           # the thread being archived or unarchived
h_body: i32 = 0          # the request's text as sent: the quote, then the reply


# LPython's wasm backend can't discard a call's result, so these wrappers
# swallow the handles we don't need.

def jstr(h: i32, path: str, dst: i32) -> None:
    r: i32
    r = j_str(h, path, dst)


def get(url: i32, msg: i32) -> None:
    r: i32
    r = http_get_ref(url, msg)


def post(url: i32, o: i32, msg: i32) -> None:
    r: i32
    r = http_post(url, o, msg)


def get_text(url: i32, msg: i32) -> None:
    r: i32
    r = http_get_text(url, msg)


def init() -> None:
    global h_pid, h_sel, h_name, h_repo, h_req, h_last, h_err, h_route, h_url, h_t1, h_t2, h_t3
    global h_working, h_idle, h_failed, h_queued, h_stopped, h_wlog_err, h_md, h_run, h_tid, h_fpath, h_files_err
    global h_fview, h_fview_err
    global h_clip, h_seltext, h_selpos, h_quote, h_lastq, h_body, h_act
    h_pid = s_new()
    h_sel = s_new()
    h_name = s_new()
    h_repo = s_new()
    h_req = s_new()
    h_last = s_new()
    h_err = s_new()
    h_route = s_new()
    h_url = s_new()
    h_t1 = s_new()
    h_t2 = s_new()
    h_t3 = s_new()
    h_working = s_new()
    s_set(h_working, "working")
    h_idle = s_new()
    s_set(h_idle, "idle")
    h_failed = s_new()
    s_set(h_failed, "failed")
    h_queued = s_new()
    s_set(h_queued, "queued")
    h_stopped = s_new()
    s_set(h_stopped, "stopped")
    h_wlog_err = s_new()
    h_fpath = s_new()
    h_files_err = s_new()
    h_fview = s_new()
    h_fview_err = s_new()
    h_md = s_new()
    h_run = s_new()
    h_tid = s_new()
    h_clip = s_new()
    h_seltext = s_new()
    h_selpos = s_new()
    h_quote = s_new()
    h_lastq = s_new()
    h_body = s_new()
    h_act = s_new()
    on_nav(NAV)
    on_select(h_seltext, h_selpos, SELECTED)
    show_address()


# ---- screens

def show_address() -> None:
    """Show what the address bar names: a project at /p/<id>, else the list."""
    if page_project(h_pid) == 1:
        enter_project()
    else:
        show_projects()


def open_project(id: i32) -> None:
    """Open project id in this window and put /p/<id> in the address bar."""
    s_copy(h_pid, id)
    s_set(h_url, "/p/")
    s_add_ref(h_url, h_pid)
    nav_push(h_url)
    enter_project()


def leave_project() -> None:
    """Close the open project's stream and forget its state."""
    global stream, project, threads, transcript, plog, wlog, files, mode, pane_tab, sending, show_archived
    if stream != 0:
        sse_close(stream)
        stream = 0
    project = take_none(project)
    threads = take_none(threads)
    transcript = take_none(transcript)
    plog = take_none(plog)
    wlog = take_none(wlog)
    files = take_none(files)
    s_clear(h_fpath)
    s_clear(h_files_err)
    close_fview()
    s_clear(h_sel)
    s_clear(h_route)
    s_clear(h_err)
    s_clear(h_wlog_err)
    s_clear(h_quote)
    mode = AUTO
    pane_tab = DIALOG
    sending = 0
    show_archived = 0


def enter_project() -> None:
    global screen, loading, stream, jump
    leave_project()
    screen = 1
    loading = 1
    s_set(h_url, "/api/projects/")
    s_add_ref(h_url, h_pid)
    get(h_url, GOT_PROJECT)
    fetch_threads()
    jump = 1
    fetch_log()
    s_set(h_url, "/api/projects/")
    s_add_ref(h_url, h_pid)
    s_add(h_url, "/events")
    stream = sse_open(h_url, EVENT)


def show_projects() -> None:
    global screen, loading
    leave_project()
    screen = 0
    loading = 1
    s_set(h_t1, "Orchestrator")
    set_title(h_t1)
    s_set(h_url, "/api/projects")
    get(h_url, GOT_PROJECTS)


# ---- requests

def fetch_threads() -> None:
    s_set(h_url, "/api/projects/")
    s_add_ref(h_url, h_pid)
    s_add(h_url, "/threads")
    if show_archived == 1:
        s_add(h_url, "?archived=1")
    get(h_url, GOT_THREADS)


def row_id(i: i32, id: i32) -> None:
    """Copy the id of the thread list's row i into id."""
    e: i32
    e = j_idx(threads, i)
    jstr(e, "id", id)
    j_free(e)


def show_activity() -> None:
    """Select nothing: the pane shows the project's activity."""
    global mode, pane_tab, jump
    s_clear(h_sel)
    if mode == TO_THREAD:
        mode = AUTO
    if pane_tab == WLOG or pane_tab == FILES:
        pane_tab = DIALOG
    jump = 1
    fetch_log()


def post_thread(id: i32, action: str, msg: i32) -> None:
    """POST /api/threads/<id>/<action>."""
    o: i32
    o = o_new()
    s_set(h_url, "/api/threads/")
    s_add_ref(h_url, id)
    s_add(h_url, "/")
    s_add(h_url, action)
    post(h_url, o, msg)


def fetch_log() -> None:
    s_set(h_url, "/api/projects/")
    s_add_ref(h_url, h_pid)
    s_add(h_url, "/log")
    get(h_url, GOT_LOG)


def fetch_transcript() -> None:
    if s_len(h_sel) == 0:
        return
    s_set(h_url, "/api/threads/")
    s_add_ref(h_url, h_sel)
    s_add(h_url, "/transcript")
    get(h_url, GOT_TRANSCRIPT)


def fetch_wlog() -> None:
    global wlog_loading
    if s_len(h_sel) == 0:
        return
    wlog_loading = 1
    s_set(h_url, "/api/threads/")
    s_add_ref(h_url, h_sel)
    s_add(h_url, "/worker-logs")
    get_text(h_url, GOT_WLOG)


def fetch_files() -> None:
    """The directory h_fpath of the selected thread's worktree, from its worker."""
    global files_loading
    if s_len(h_sel) == 0:
        return
    files_loading = 1
    s_set(h_url, "/api/threads/")
    s_add_ref(h_url, h_sel)
    s_add(h_url, "/files?path=")
    s_add_enc(h_url, h_fpath)
    get(h_url, GOT_FILES)


def file_url(dst: i32, name: i32) -> None:
    """dst = the URL of file name in directory h_fpath of the selected thread's worktree."""
    s_set(dst, "/api/threads/")
    s_add_ref(dst, h_sel)
    s_add(dst, "/files/raw?path=")
    s_copy(h_t3, h_fpath)
    if s_len(h_fpath) > 0:
        s_add(h_t3, "/")
    s_add_ref(h_t3, name)
    s_add_enc(dst, h_t3)


def close_fview() -> None:
    """Back to the listing from a markdown file."""
    global fview, fview_req
    fview = take_none(fview)
    fview_req = 0
    s_clear(h_fview)
    s_clear(h_fview_err)


def take_none(old: i32) -> i32:
    if old != 0:
        j_free(old)
    return 0


def find_text(seq: i32, dst: i32) -> i32:
    """Write the text of the shown event with this seq (in the transcript, or
    in project activity) to dst; 1 if it is there."""
    src: i32 = plog
    n: i32 = 0
    i: i32
    e: i32
    found: i32 = 0
    if s_len(h_sel) > 0:
        src = transcript
    if src != 0:
        n = j_len(src, "")
    for i in range(n):
        e = j_idx(src, i)
        if found == 0 and j_int(e, "seq") == seq:
            jstr(e, "text", dst)
            found = 1
        j_free(e)
    return found


def take(old: i32, new: i32) -> i32:
    """Swap in a fresh JSON reply, freeing the old one. On error keep the old
    one and put the error text in h_err."""
    if j_ok(new) == 0:
        j_err(new, h_err)
        j_free(new)
        return old
    s_clear(h_err)
    if old != 0:
        j_free(old)
    return new


# ---- Update

def land() -> None:
    """New pane content has arrived. Content navigation asked for opens at the
    bottom; content that streamed in keeps the bottom in view only if the
    reader was already there, so scrolling up to read is left alone."""
    global jump
    if jump == 1:
        jump = 0
        scroll_end("pane")
    else:
        scroll_follow("pane")


def update(msg: i32, arg: i32) -> None:
    global projects, project, threads, transcript, plog, loading, show_new, creating, sending
    global mode, pending, pane_tab, wlog, wlog_loading, show_archived, files, files_loading, jump
    global copied, selecting, fview_req
    e: i32 = 0
    k: i32 = 0
    o: i32 = 0
    if msg == GOT_PROJECTS:
        if screen == 0:
            loading = 0
        projects = take(projects, arg)
    elif msg == OPEN:
        e = j_idx(projects, arg)
        jstr(e, "id", h_t1)
        j_free(e)
        open_project(h_t1)
    elif msg == NEW_SHOW:
        show_new = 1
    elif msg == NEW_HIDE:
        show_new = 0
    elif msg == NAME_IN:
        s_copy(h_name, arg)
    elif msg == REPO_IN:
        s_copy(h_repo, arg)
    elif msg == CREATE:
        if s_len(h_name) > 0 and creating == 0:
            creating = 1
            o = o_new()
            o_str(o, "name", h_name)
            o_str(o, "repo_url", h_repo)
            s_set(h_url, "/api/projects")
            post(h_url, o, CREATED)
    elif msg == CREATED:
        creating = 0
        if j_ok(arg) == 1:
            show_new = 0
            jstr(arg, "id", h_t1)
            s_clear(h_name)
            s_clear(h_repo)
            open_project(h_t1)
        else:
            j_err(arg, h_err)
        j_free(arg)
    elif msg == GOT_PROJECT:
        project = take(project, arg)
        if project != 0 and screen == 1:
            jstr(project, "name", h_t1)
            set_title(h_t1)
    elif msg == GOT_THREADS:
        if screen == 1:
            loading = 0
            threads = take(threads, arg)
        else:
            j_free(arg)
    elif msg == GOT_LOG:
        plog = take(plog, arg)
        if s_len(h_sel) == 0 and pane_tab != WLOG and pane_tab != FILES:
            land()
    elif msg == SELECT:
        e = j_idx(threads, arg)
        jstr(e, "id", h_sel)
        j_free(e)
        if mode == AUTO and show_archived == 0:
            mode = TO_THREAD
        jump = 1
        fetch_transcript()
        wlog = take_none(wlog)
        s_clear(h_wlog_err)
        if pane_tab == WLOG:
            fetch_wlog()
        files = take_none(files)
        s_clear(h_fpath)
        s_clear(h_files_err)
        close_fview()
        if pane_tab == FILES:
            fetch_files()
    elif msg == ACTIVITY:
        show_activity()
    elif msg == GOT_TRANSCRIPT:
        transcript = take(transcript, arg)
        if pane_tab != WLOG and pane_tab != FILES:
            land()
    elif msg == REQ_IN:
        s_copy(h_req, arg)
    elif msg == MODE:
        mode = arg
    elif msg == SEND:
        # The box clears at once: the control plane takes the request and
        # returns, and what becomes of it arrives on the event stream.
        if s_len(h_req) > 0 and sending == 0:
            sending = 1
            s_copy(h_last, h_req)
            s_copy(h_lastq, h_quote)
            s_clear(h_req)
            s_clear(h_quote)
            s_clear(h_err)
            # A reply to a selection goes out as markdown: the quote, then the reply.
            s_clear(h_body)
            if s_len(h_lastq) > 0:
                s_quote(h_body, h_lastq)
                s_add(h_body, "\n\n")
            s_add_ref(h_body, h_last)
            o = o_new()
            o_str(o, "text", h_body)
            if mode == TO_THREAD and s_len(h_sel) > 0:
                o_lit(o, "mode", "thread")
                o_str(o, "target", h_sel)
            elif mode == NEW_THREAD:
                o_lit(o, "mode", "new")
            else:
                o_lit(o, "mode", "auto")
                s_set(h_route, "routing…")
            s_set(h_url, "/api/projects/")
            s_add_ref(h_url, h_pid)
            s_add(h_url, "/requests")
            post(h_url, o, SENT)
    elif msg == SENT:
        # The router's line comes from its "route" event. A spawned thread
        # never takes the window with it: the person stays where they put the
        # pane and opens a thread only by clicking it in the list.
        sending = 0
        if j_ok(arg) == 0:
            j_err(arg, h_err)
            if s_len(h_req) == 0:
                s_copy(h_req, h_last)
            if s_len(h_quote) == 0:
                s_copy(h_quote, h_lastq)
        j_free(arg)
    elif msg == COPY:
        # arg is the reply's seq; its raw markdown is what gets copied.
        if find_text(arg, h_clip) == 1:
            clip_copy(h_clip, COPIED, arg, COPY_FAILED)
    elif msg == COPIED:
        copied = arg
        after(1500, COPIED_DONE, arg)
    elif msg == COPIED_DONE:
        if copied == arg:
            copied = -1
    elif msg == COPY_FAILED:
        s_set(h_err, "Could not copy: ")
        s_add_ref(h_err, arg)
    elif msg == SELECTED:
        selecting = arg
    elif msg == QUOTE:
        s_copy(h_quote, h_seltext)
        if s_len(h_sel) > 0 and show_archived == 0:
            mode = TO_THREAD
        sel_clear()
        focus_end("req")
    elif msg == UNQUOTE:
        s_clear(h_quote)
        focus_end("req")
    elif msg == EVENT:
        k = j_int(arg, "kind")
        if k == K_ROUTE:
            jstr(arg, "text", h_route)
        elif k == K_ERROR and j_has(arg, "thread_id") == 0:
            jstr(arg, "text", h_err)
        j_free(arg)
        if pending == 0:
            pending = 1
            after(150, REFRESH, 0)
    elif msg == REFRESH:
        pending = 0
        if screen == 1:
            fetch_threads()
            if s_len(h_sel) > 0:
                fetch_transcript()
            else:
                fetch_log()
    elif msg == STOP:
        if s_len(h_sel) > 0:
            o = o_new()
            s_set(h_url, "/api/threads/")
            s_add_ref(h_url, h_sel)
            s_add(h_url, "/stop")
            post(h_url, o, STOPPED)
    elif msg == ARCHIVE:
        if s_len(h_sel) > 0:
            s_copy(h_act, h_sel)
            post_thread(h_act, "archive", ARCHIVED)
    elif msg == ARCHIVE_ROW:
        row_id(arg, h_act)
        post_thread(h_act, "archive", ARCHIVED)
    elif msg == ARCHIVED:
        # The control plane stopped the thread if it was running; it leaves
        # the list, so if it was open the pane goes back to the project's activity.
        if j_ok(arg) == 1:
            if s_eq(h_act, h_sel) == 1:
                show_activity()
            fetch_threads()
        else:
            j_err(arg, h_err)
        j_free(arg)
    elif msg == UNARCHIVE:
        if s_len(h_sel) > 0:
            s_copy(h_act, h_sel)
            post_thread(h_act, "unarchive", UNARCHIVED)
    elif msg == UNARCHIVE_ROW:
        row_id(arg, h_act)
        post_thread(h_act, "unarchive", UNARCHIVED)
    elif msg == UNARCHIVED:
        # The open thread goes back to the live list, still selected; one
        # unarchived from its row just leaves the archived list.
        if j_ok(arg) == 1:
            if s_eq(h_act, h_sel) == 1:
                show_archived = 0
                fetch_transcript()
            fetch_threads()
        else:
            j_err(arg, h_err)
        j_free(arg)
    elif msg == SHOW_ARCHIVED:
        show_archived = arg
        threads = take_none(threads)
        show_activity()
        fetch_threads()
    elif msg == STOPPED:
        j_free(arg)
        fetch_threads()
    elif msg == TAB:
        pane_tab = arg
        if pane_tab == WLOG and wlog == 0:
            jump = 1
            fetch_wlog()
        if pane_tab == FILES and files == 0:
            fetch_files()
        scroll_end("pane")
    elif msg == WLOG_REFRESH:
        fetch_wlog()
    elif msg == GOT_WLOG:
        # A reply for a thread that is no longer selected lands harmlessly:
        # SELECT cleared the log, and the next fetch replaces it.
        wlog_loading = 0
        if j_ok(arg) == 1:
            s_clear(h_wlog_err)
            wlog = take_none(wlog)
            wlog = arg
        else:
            j_err(arg, h_wlog_err)
            j_free(arg)
        if pane_tab == WLOG:
            land()
    elif msg == GOT_FILES:
        # Like the worker log: a late reply for another thread is replaced by
        # the next fetch. The listing names its own (normalized) path.
        files_loading = 0
        if j_ok(arg) == 1:
            s_clear(h_files_err)
            files = take_none(files)
            files = arg
            jstr(files, "path", h_fpath)
        else:
            j_err(arg, h_files_err)
            j_free(arg)
    elif msg == FILE_OPEN:
        # arg indexes a directory entry of the listing shown; files download
        # through their link instead.
        o = j_at(files, "entries")
        e = j_idx(o, arg)
        jstr(e, "name", h_t1)
        j_free(e)
        j_free(o)
        if s_len(h_fpath) > 0:
            s_add(h_fpath, "/")
        s_add_ref(h_fpath, h_t1)
        fetch_files()
    elif msg == FILE_VIEW:
        # arg indexes a markdown file of the listing shown: read it as text
        # and show it rendered, in the pane.
        o = j_at(files, "entries")
        e = j_idx(o, arg)
        jstr(e, "name", h_t1)
        j_free(e)
        j_free(o)
        close_fview()
        s_copy(h_fview, h_fpath)
        if s_len(h_fpath) > 0:
            s_add(h_fview, "/")
        s_add_ref(h_fview, h_t1)
        file_url(h_url, h_t1)
        fview_req = http_get_text(h_url, GOT_FVIEW)
    elif msg == GOT_FVIEW:
        # A reply after Back, or for a file or thread no longer shown, is
        # dropped: only the read last asked for lands.
        if arg != fview_req:
            j_free(arg)
        else:
            fview_req = 0
            if j_ok(arg) == 1:
                fview = arg
            else:
                j_err(arg, h_fview_err)
                j_free(arg)
    elif msg == FVIEW_CLOSE:
        close_fview()
    elif msg == FILES_UP:
        s_add(h_fpath, "/..")
        fetch_files()
    elif msg == FILES_REFRESH:
        fetch_files()
    elif msg == HOME:
        s_set(h_url, "/")
        nav_push(h_url)
        show_projects()
    elif msg == NAV:
        show_address()


# ---- View: shared

def state_badge(h: i32) -> None:
    if s_eq(h, h_working) == 1:
        badge("working", "")
    elif s_eq(h, h_failed) == 1:
        badge("failed", "destructive")
    elif s_eq(h, h_queued) == 1:
        badge("queued", "secondary")
    elif s_eq(h, h_stopped) == 1:
        badge("stopped", "outline")
    elif s_eq(h, h_idle) == 1:
        badge("idle", "secondary")
    else:
        el("span")
        cls("badge")
        cls("outline")
        text_ref(h)
        end()


def error_banner() -> None:
    if s_len(h_err) > 0:
        el("div")
        cls("alert")
        cls("destructive")
        attr("role", "alert")
        text_ref(h_err)
        end()


# ---- View: project list

def project_list() -> None:
    n: i32 = 0
    i: i32
    e: i32 = 0
    div("page")
    div("topbar")
    el("h1")
    text("Projects")
    end()
    btn("New project", "", NEW_SHOW, 0)
    end()
    error_banner()
    if projects != 0:
        n = j_len(projects, "")
    if loading == 1:
        el("p")
        cls("muted")
        text("Loading…")
        end()
    elif n == 0:
        card_open()
        card_header("No projects yet", "A project is a repo plus the threads working on it.")
        card_footer_open()
        btn("Create a project", "", NEW_SHOW, 0)
        end()
        end()
    div("grid")
    for i in range(n):
        e = j_idx(projects, i)
        jstr(e, "id", h_t3)
        el("div")
        cls("card")
        cls("project-card")
        key(i + 1)
        on_click(OPEN, i)
        el("div")
        cls("card-header")
        el("h3")
        cls("card-title")
        jstr(e, "name", h_t1)
        text_ref(h_t1)
        end()
        el("p")
        cls("card-description")
        jstr(e, "repo_url", h_t2)
        if s_len(h_t2) > 0:
            text_ref(h_t2)
        else:
            text("no repo")
        end()
        end()
        el("div")
        cls("card-footer")
        span("muted mono")
        text_ref(h_t3)
        end()
        btn_open("outline", OPEN, i)
        cls("sm")
        text("Open")
        end()
        end()
        end()
        j_free(e)
    end()
    end()
    if show_new == 1:
        new_project_dialog()


def new_project_dialog() -> None:
    dialog_open("New project", "A repo plus the threads working on it.", NEW_HIDE)
    el("div")
    cls("form")
    label("Name")
    input_open("my-app", h_name, NAME_IN)
    autofocus()
    on_enter(CREATE, 0)
    end()
    label("Git repo URL (optional)")
    input_open("https://github.com/you/repo", h_repo, REPO_IN)
    on_enter(CREATE, 0)
    end()
    end()
    dialog_footer_open()
    btn("Cancel", "outline", NEW_HIDE, 0)
    btn_open("", CREATE, 0)
    if creating == 1:
        text("Creating…")
    else:
        text("Create")
    end()
    end()
    end()


# ---- View: project window

def thread_list() -> None:
    n: i32 = 0
    i: i32
    e: i32 = 0
    el("aside")
    cls("threads")
    div("threads-head")
    span("title")
    if show_archived == 1:
        text("Archived")
    else:
        text("Threads")
    end()
    if threads != 0:
        n = j_len(threads, "")
    el("span")
    cls("badge")
    cls("secondary")
    num(n)
    end()
    span("grow")
    end()
    if show_archived == 1:
        btn_open("ghost", SHOW_ARCHIVED, 0)
        cls("sm")
        text("← Threads")
        end()
    else:
        btn_open("ghost", SHOW_ARCHIVED, 1)
        cls("sm")
        text("Archived")
        end()
    end()
    el("button")
    cls("thread")
    cls_if("active", s_len(h_sel) == 0)
    key(-1)
    on_click(ACTIVITY, 0)
    div("thread-top")
    span("thread-title")
    text("Project activity")
    end()
    end()
    el("div")
    cls("thread-summary")
    text("Router decisions and every thread's events")
    end()
    end()
    for i in range(n):
        e = j_idx(threads, i)
        jstr(e, "id", h_t3)
        # The row holds the thread's button and, beside it rather than inside
        # it, the archive button: a click there archives without opening.
        el("div")
        cls("thread-row")
        cls_if("child", j_has(e, "parent_id") == 1)
        key(i + 1)
        el("button")
        cls("thread")
        cls_if("active", s_eq(h_t3, h_sel) == 1)
        on_click(SELECT, i)
        div("thread-top")
        span("thread-title")
        jstr(e, "title", h_t1)
        text_ref(h_t1)
        end()
        jstr(e, "state", h_t2)
        state_badge(h_t2)
        end()
        jstr(e, "activity", h_t1)
        if s_len(h_t1) > 0:
            el("div")
            cls("thread-activity")
            span("activity-dot")
            end()
            span("mono")
            text_ref(h_t1)
            end()
            end()
        else:
            el("div")
            cls("thread-summary")
            jstr(e, "summary", h_t1)
            if s_len(h_t1) > 0:
                text_ref(h_t1)
            else:
                jstr(e, "request", h_t1)
                text_ref(h_t1)
            end()
        span("thread-id mono")
        text_ref(h_t3)
        end()
        end()
        el("button")
        cls("thread-archive")
        if show_archived == 1:
            on_click(UNARCHIVE_ROW, i)
            attr("title", "Unarchive")
            attr("aria-label", "Unarchive")
            text("Unarchive")
        else:
            on_click(ARCHIVE_ROW, i)
            attr("title", "Stops the thread if it is running and hides it from the list")
            attr("aria-label", "Archive")
            text("Archive")
        end()
        end()
        j_free(e)
    if show_archived == 1 and n == 0:
        el("p")
        cls("muted")
        cls("empty")
        text("No archived threads.")
        end()
    end()


def is_tool(e: i32) -> i32:
    k: i32
    k = j_int(e, "kind")
    if k == K_TOOL_CALL or k == K_TOOL_RESULT:
        return 1
    return 0


def thread_index(id: i32) -> i32:
    """Where thread id is in the directory, or -1."""
    n: i32 = 0
    i: i32
    e: i32
    if threads != 0:
        n = j_len(threads, "")
    for i in range(n):
        e = j_idx(threads, i)
        jstr(e, "id", h_tid)
        j_free(e)
        if s_eq(h_tid, id) == 1:
            return i
    return -1


def thread_link(id: i32) -> None:
    """Thread id by its title; clicking it opens the thread."""
    i: i32
    e: i32
    i = thread_index(id)
    el("button")
    cls("ev-thread")
    if i >= 0:
        on_click(SELECT, i)
        e = j_idx(threads, i)
        jstr(e, "title", h_tid)
        j_free(e)
        text_ref(h_tid)
    else:
        text_ref(id)
    end()


def thread_tag(e: i32, show_thread: i32) -> None:
    if show_thread == 1:
        if j_str(e, "thread_id", h_t2) == 1:
            thread_link(h_t2)


def working_now() -> None:
    """Project activity's live line per working thread: its title and what it
    is doing, instead of every thread's reasoning."""
    n: i32 = 0
    i: i32
    e: i32
    if threads != 0:
        n = j_len(threads, "")
    div("working")
    for i in range(n):
        e = j_idx(threads, i)
        jstr(e, "activity", h_t1)
        if s_len(h_t1) > 0:
            el("button")
            cls("working-row")
            key(i + 1)
            on_click(SELECT, i)
            span("activity-dot")
            end()
            span("working-title")
            jstr(e, "title", h_t2)
            text_ref(h_t2)
            end()
            span("working-now mono")
            text_ref(h_t1)
            end()
            end()
        j_free(e)
    end()


def event_row(e: i32, show_thread: i32) -> None:
    """One dialog entry. While the model streams, its reasoning and reply show
    as they grow; finished reasoning is grouped by thinking_group."""
    k: i32
    live: i32
    k = j_int(e, "kind")
    live = j_int(e, "live")
    el("div")
    cls("ev")
    key(j_int(e, "seq"))
    cls_if("ev-user", k == K_USER)
    cls_if("ev-request", k == K_REQUEST)
    cls_if("ev-assistant", k == K_ASSISTANT)
    cls_if("ev-status", k == K_STATUS)
    cls_if("ev-peer", k == K_PEER)
    cls_if("ev-error", k == K_ERROR)
    cls_if("ev-route", k == K_ROUTE)
    cls_if("ev-thinking", k == K_THINKING)
    cls_if("ev-result", k == K_RESULT)
    cls_if("ev-live", live == 1)
    div("ev-label")
    jstr(e, "label", h_t1)
    text_ref(h_t1)
    thread_tag(e, show_thread)
    end()
    jstr(e, "text", h_t1)
    if k == K_ASSISTANT or k == K_PEER or k == K_RESULT or k == K_USER:
        # Replies get a copy button; V's own messages render as markdown too,
        # so a reply to a selection shows its quote as a quote.
        if live == 0 and k != K_USER:
            copy_button(j_int(e, "seq"))
        el("div")
        cls("ev-text")
        cls("md")
        md(h_t1)
        end()
    else:
        el("div")
        cls("ev-text")
        text_ref(h_t1)
        end()
    end()


def copy_button(seq: i32) -> None:
    """The reply's copy icon (its raw markdown), a check for a moment after."""
    btn_open("ghost", COPY, seq)
    cls("icon")
    cls("sm")
    cls("ev-copy")
    if copied == seq:
        attr("title", "Copied")
        attr("aria-label", "Copied")
        span("icon icon-check")
        end()
    else:
        attr("title", "Copy")
        attr("aria-label", "Copy")
        span("icon icon-copy")
        end()
    end()


def reply_button() -> None:
    """Over text selected in the pane: reply to it, quoted in the request box."""
    if selecting == 0:
        return
    btn_open("", QUOTE, 0)
    cls("sm")
    cls("sel-reply")
    attr_ref("style", h_selpos)
    span("icon icon-reply")
    end()
    text("Reply")
    end()


def quote_chip() -> None:
    """The selection the request replies to, with a button to drop it."""
    if s_len(h_quote) == 0:
        return
    div("quote")
    span("icon icon-reply")
    end()
    span("quote-text")
    attr_ref("title", h_quote)
    text_ref(h_quote)
    end()
    btn_open("ghost", UNQUOTE, 0)
    cls("icon")
    cls("sm")
    attr("title", "Don't quote this")
    attr("aria-label", "Don't quote this")
    span("icon icon-x")
    end()
    end()
    end()


def md(h: i32) -> None:
    """The markdown in handle h, as elements. A reply still streaming in is
    parsed whole on each render, so it reads right at every step."""
    n: i32
    i: i32
    op: i32
    n = md_parse(h)
    for i in range(n):
        op = md_op(i)
        if op == MD_END:
            end()
        elif op == MD_TEXT:
            md_str(i, h_md)
            text_ref(h_md)
        elif op == MD_HREF:
            md_str(i, h_md)
            attr_ref("href", h_md)
        elif op == MD_START:
            md_str(i, h_md)
            attr_ref("start", h_md)
        elif op == MD_ALIGN_C:
            cls("center")
        elif op == MD_ALIGN_R:
            cls("right")
        else:
            md_open(op)


def md_open(op: i32) -> None:
    """Open the element for markdown op code op (TAGS in ui/markdown.js)."""
    if op == 10:
        el("p")
    elif op == 11:
        el("h1")
    elif op == 12:
        el("h2")
    elif op == 13:
        el("h3")
    elif op == 14:
        el("h4")
    elif op == 15:
        el("h5")
    elif op == 16:
        el("h6")
    elif op == 17:
        el("ul")
    elif op == 18:
        el("ol")
    elif op == 19:
        el("li")
    elif op == 20:
        el("pre")
    elif op == 21:
        el("code")
    elif op == 22:
        el("blockquote")
    elif op == 23:
        el("hr")
    elif op == 24:
        el("table")
    elif op == 25:
        el("thead")
    elif op == 26:
        el("tbody")
    elif op == 27:
        el("tr")
    elif op == 28:
        el("th")
    elif op == 29:
        el("td")
    elif op == 30:
        el("strong")
    elif op == 31:
        el("em")
    elif op == 32:
        el("del")
    elif op == 33:
        el("a")
    elif op == 34:
        el("br")
    elif op == 35:
        el("div")
        cls("md-code")
    elif op == 36:
        el("div")
        cls("md-lang")
    else:
        el("div")
        cls("md-table")


def is_thought(e: i32) -> i32:
    """Finished reasoning (a live row still streams on its own)."""
    if j_int(e, "kind") == K_THINKING and j_int(e, "live") == 0:
        return 1
    return 0


def hidden(e: i32, activity: i32) -> i32:
    """Left out of the Dialog: tool rows (under Tools) and state changes
    (the badges show them). Project activity also leaves out reasoning and
    replies still streaming: it is about requests, routing and answers, and
    working_now says what each thread is doing."""
    k: i32
    k = j_int(e, "kind")
    if is_tool(e) == 1 or k == K_STATE:
        return 1
    if activity == 1 and (k == K_THINKING or j_int(e, "live") == 1):
        return 1
    return 0


def thought_run(src: i32, a: i32, n: i32) -> i32:
    """The end (exclusive) of the run of reasoning starting at src[a]: the
    steps the Dialog shows back to back, with only hidden rows between them,
    all from one thread."""
    b: i32
    i: i32
    e: i32
    b = a + 1
    i = a + 1
    e = j_idx(src, a)
    jstr(e, "thread_id", h_run)
    j_free(e)
    while i < n:
        e = j_idx(src, i)
        if is_thought(e) == 1:
            jstr(e, "thread_id", h_t2)
            if s_eq(h_t2, h_run) == 0:
                j_free(e)
                return b
            b = i + 1
        elif hidden(e, 0) == 0:
            j_free(e)
            return b
        j_free(e)
        i = i + 1
    return b


def thinking_group(src: i32, a: i32, b: i32, show_thread: i32) -> None:
    """A run of reasoning steps, collapsed to one line (the latest step) until
    clicked."""
    i: i32
    e: i32
    steps: i32 = 0
    for i in range(a, b):
        e = j_idx(src, i)
        steps = steps + is_thought(e)
        j_free(e)
    e = j_idx(src, a)
    el("details")
    cls("tool")
    cls("thinking")
    key(j_int(e, "seq"))
    el("summary")
    span("tool-name")
    text("Thinking")
    end()
    if steps > 1:
        span("steps")
        num(steps)
        text(" steps")
        end()
    thread_tag(e, show_thread)
    j_free(e)
    e = j_idx(src, b - 1)
    span("tool-preview")
    jstr(e, "text", h_t1)
    text_ref(h_t1)
    end()
    j_free(e)
    end()
    el("ol")
    cls("tool-body")
    cls("thoughts")
    for i in range(a, b):
        e = j_idx(src, i)
        if is_thought(e) == 1:
            el("li")
            jstr(e, "text", h_t1)
            text_ref(h_t1)
            end()
        j_free(e)
    end()
    end()


def tool_row(e: i32, show_thread: i32) -> None:
    """One tool call or result, collapsed to a single line until clicked."""
    k: i32
    k = j_int(e, "kind")
    el("details")
    cls("tool")
    cls_if("tool-result", k == K_TOOL_RESULT)
    key(j_int(e, "seq"))
    el("summary")
    span("tool-name mono")
    jstr(e, "label", h_t1)
    text_ref(h_t1)
    end()
    thread_tag(e, show_thread)
    span("tool-preview mono")
    jstr(e, "text", h_t1)
    text_ref(h_t1)
    end()
    end()
    el("div")
    cls("tool-body")
    cls("mono")
    text_ref(h_t1)
    end()
    end()


def selected_thread() -> i32:
    """The selected thread's directory entry (a JSON handle the caller frees),
    or 0 before the directory has it."""
    n: i32 = 0
    i: i32 = 0
    e: i32 = 0
    if threads != 0:
        n = j_len(threads, "")
    while i < n:
        e = j_idx(threads, i)
        jstr(e, "id", h_t3)
        if s_eq(h_t3, h_sel) == 1:
            return e
        j_free(e)
        i = i + 1
    return 0


def thread_head(t: i32) -> None:
    """Title, state, Stop and Archive (Unarchive once archived) for the selected thread, and a line saying what it
    is doing right now (the running tool, or "thinking") while it works."""
    div("pane-head")
    div("pane-title")
    span("title")
    if t != 0:
        jstr(t, "title", h_t1)
        text_ref(h_t1)
    else:
        text_ref(h_sel)
    end()
    if t != 0:
        jstr(t, "state", h_t2)
        state_badge(h_t2)
    span("mono muted")
    text_ref(h_sel)
    end()
    end()
    div("pane-actions")
    if t != 0 and j_has(t, "archived_at") == 1:
        btn_open("ghost", UNARCHIVE, 0)
        cls("sm")
        text("Unarchive")
        end()
    else:
        btn_open("ghost", STOP, 0)
        cls("sm")
        text("Stop thread")
        end()
        btn_open("ghost", ARCHIVE, 0)
        cls("sm")
        attr("title", "Stops the thread if it is running and hides it from the list")
        text("Archive")
        end()
    end()
    end()
    # Always there (empty, hidden by CSS, while idle): the pane's children
    # keep their places, so a thread starting work doesn't rebuild the
    # events list below and throw away its scroll position.
    div("now")
    if t != 0:
        jstr(t, "activity", h_t1)
        if s_len(h_t1) > 0:
            span("activity-dot")
            end()
            span("now-label")
            text("Now")
            end()
            span("now-text mono")
            text_ref(h_t1)
            end()
    end()


def tab_open(active: bool, arg: i32) -> None:
    el("button")
    cls("tab")
    attr("role", "tab")
    aria("aria-selected", active)
    on_click(TAB, arg)


def pane_tabs(dialog: i32, tools: i32, logs: bool) -> None:
    """Dialog (the conversation) and Tools (calls and their output), with
    counts, and for a thread (logs set) Files (its worktree) and Worker log
    (the runner's stdout and stderr)."""
    div("pane-tabs")
    tabs_list_open()
    tab_open(pane_tab == DIALOG, DIALOG)
    text("Dialog")
    span("tab-count")
    num(dialog)
    end()
    end()
    tab_open(pane_tab == TOOLS, TOOLS)
    text("Tools")
    span("tab-count")
    num(tools)
    end()
    end()
    if logs:
        tab_open(pane_tab == FILES, FILES)
        text("Files")
        end()
        tab_open(pane_tab == WLOG, WLOG)
        text("Worker log")
        end()
    end()
    end()


def worker_log() -> None:
    """The selected thread's worker log: the last lines its runner wrote."""
    div("wlog-bar")
    span("muted")
    text("Last 300 lines of this thread's worker")
    end()
    btn_open("ghost", WLOG_REFRESH, 0)
    cls("sm")
    if wlog_loading == 1:
        text("Loading…")
    else:
        text("Refresh")
    end()
    end()
    if s_len(h_wlog_err) > 0:
        el("div")
        cls("alert")
        cls("destructive")
        attr("role", "alert")
        text_ref(h_wlog_err)
        end()
    elif wlog != 0:
        el("pre")
        cls("wlog")
        jstr(wlog, "text", h_t1)
        if s_len(h_t1) > 0:
            text_ref(h_t1)
        else:
            text("(empty)")
        end()


def size_text(n: i32) -> None:
    if n < 1024:
        num(n)
        text(" B")
    elif n < 1048576:
        num(n // 1024)
        text(" KB")
    else:
        num(n // 1048576)
        text(" MB")


def file_view() -> None:
    """The markdown file h_fview, rendered in the pane, with Back to its
    directory and a Download link (the browser saves it; the page stays)."""
    div("wlog-bar")
    span("mono")
    text("/")
    text_ref(h_fview)
    end()
    div("pane-actions")
    btn_open("ghost", FVIEW_CLOSE, 0)
    cls("sm")
    text("Back")
    end()
    s_set(h_t2, "/api/threads/")
    s_add_ref(h_t2, h_sel)
    s_add(h_t2, "/files/raw?path=")
    s_add_enc(h_t2, h_fview)
    el("a")
    cls("btn")
    cls("ghost")
    cls("sm")
    attr("id", "fview-download")
    attr_ref("href", h_t2)
    attr_ref("download", h_fview)
    text("Download")
    end()
    end()
    end()
    if s_len(h_fview_err) > 0:
        el("div")
        cls("alert")
        cls("destructive")
        attr("role", "alert")
        text_ref(h_fview_err)
        end()
    elif fview_req != 0:
        el("p")
        cls("muted")
        cls("empty")
        text("Loading…")
        end()
    elif fview != 0:
        el("div")
        cls("fview")
        cls("md")
        jstr(fview, "text", h_t1)
        md(h_t1)
        end()


def file_list() -> None:
    """The selected thread's worktree, one directory at a time, read from the
    worker that holds it. A directory opens in place; a markdown file opens
    rendered in the pane (file_view); any other file is a download link (the
    browser saves it; the page stays)."""
    i: i32
    n: i32 = 0
    e: i32
    ents: i32
    if s_len(h_fview) > 0:
        file_view()
        return
    div("wlog-bar")
    span("mono")
    text("/")
    text_ref(h_fpath)
    end()
    div("pane-actions")
    if s_len(h_fpath) > 0:
        btn_open("ghost", FILES_UP, 0)
        cls("sm")
        text("Up")
        end()
    btn_open("ghost", FILES_REFRESH, 0)
    cls("sm")
    if files_loading == 1:
        text("Loading…")
    else:
        text("Refresh")
    end()
    end()
    end()
    if s_len(h_files_err) > 0:
        el("div")
        cls("alert")
        cls("destructive")
        attr("role", "alert")
        text_ref(h_files_err)
        end()
        return
    if files == 0:
        return
    ents = j_at(files, "entries")
    n = j_len(ents, "")
    if n == 0:
        j_free(ents)
        el("p")
        cls("muted")
        cls("empty")
        text("This directory is empty.")
        end()
        return
    div("files")
    for i in range(n):
        e = j_idx(ents, i)
        jstr(e, "name", h_t1)
        if j_int(e, "dir") == 1:
            el("button")
            cls("file")
            key(i)
            on_click(FILE_OPEN, i)
            span("file-name")
            text_ref(h_t1)
            text("/")
            end()
            end()
        elif is_md(h_t1) == 1:
            el("button")
            cls("file")
            key(i)
            on_click(FILE_VIEW, i)
            span("file-name")
            text_ref(h_t1)
            end()
            span("file-size muted")
            size_text(j_int(e, "size"))
            end()
            end()
        else:
            file_url(h_t2, h_t1)
            el("a")
            cls("file")
            key(i)
            attr_ref("href", h_t2)
            attr_ref("download", h_t1)
            attr("title", "Download")
            span("file-name")
            text_ref(h_t1)
            end()
            span("file-size muted")
            size_text(j_int(e, "size"))
            end()
            end()
        j_free(e)
    end()
    j_free(ents)


def pane() -> None:
    n: i32 = 0
    i: i32
    e: i32 = 0
    t: i32 = 0
    tools: i32 = 0
    shown: i32 = 0
    src: i32
    activity: i32
    src = plog
    activity = 1
    if s_len(h_sel) > 0:
        src = transcript
        activity = 0
    if src != 0:
        n = j_len(src, "")
    said: i32 = 0
    j: i32
    for i in range(n):
        e = j_idx(src, i)
        tools = tools + is_tool(e)
        if hidden(e, activity) == 0:
            said = said + 1
        j_free(e)
    el("section")
    cls("pane")
    if activity == 1:
        div("pane-head")
        span("title")
        text("Project activity")
        end()
        end()
    else:
        t = selected_thread()
        thread_head(t)
        if t != 0:
            j_free(t)
    pane_tabs(said, tools, activity == 0)
    if activity == 1 and pane_tab == DIALOG:
        working_now()
    el("div")
    cls("events")
    cls_if("events-tools", pane_tab == TOOLS)
    attr("id", "pane")
    if pane_tab == WLOG:
        worker_log()
        end()
        end()
        return
    if pane_tab == FILES:
        file_list()
        end()
        end()
        return
    j = 0
    while j < n:
        e = j_idx(src, j)
        if pane_tab == TOOLS:
            if is_tool(e) == 1:
                tool_row(e, activity)
                shown = shown + 1
            j = j + 1
        elif hidden(e, activity) == 1:
            j = j + 1
        elif is_thought(e) == 1:
            i = thought_run(src, j, n)
            thinking_group(src, j, i, activity)
            shown = shown + 1
            j = i
        else:
            event_row(e, activity)
            shown = shown + 1
            j = j + 1
        j_free(e)
    if shown == 0:
        el("p")
        cls("muted")
        cls("empty")
        if n > 0 and pane_tab == TOOLS:
            text("No tool calls yet.")
        elif n > 0:
            text("Nothing said yet. Tool calls are under Tools.")
        elif activity == 1:
            text("Nothing yet. Type a request below; the router spawns threads for it.")
        else:
            text("Waiting for this thread's first event…")
        end()
    end()
    end()


def request_box() -> None:
    el("footer")
    cls("composer")
    # Project activity shows the router's decisions in its log; with a thread
    # open, the last one shows here instead.
    if s_len(h_route) > 0 and s_len(h_sel) > 0:
        div("route")
        attr_ref("title", h_route)
        span("route-label")
        text("Router")
        end()
        text_ref(h_route)
        end()
    error_banner()
    quote_chip()
    div("composer-row")
    toggle_group_open()
    toggle("Router decides", mode == AUTO, MODE, AUTO)
    if s_len(h_sel) > 0 and show_archived == 0:
        toggle("This thread", mode == TO_THREAD, MODE, TO_THREAD)
    toggle("New thread", mode == NEW_THREAD, MODE, NEW_THREAD)
    end()
    end()
    div("composer-row")
    el("input")
    cls("input")
    cls("grow")
    attr("id", "req")
    if s_len(h_quote) > 0:
        attr("placeholder", "Reply to the quote…")
    elif mode == TO_THREAD and s_len(h_sel) > 0:
        attr("placeholder", "Message this thread…")
    else:
        attr("placeholder", "What should get done? A list fans out into one thread per item.")
    from_input()
    end()
    btn_open("", SEND, 0)
    if sending == 1:
        text("Sending…")
    else:
        text("Send")
    end()
    end()
    end()


def from_input() -> None:
    # value + events for the request input
    value_ref(h_req)
    on_input(REQ_IN)
    on_enter(SEND, 0)
    autofocus()


def project_window() -> None:
    div("app")
    el("header")
    cls("appbar")
    btn_open("ghost", HOME, 0)
    cls("sm")
    text("← All projects")
    end()
    el("h1")
    if project != 0:
        jstr(project, "name", h_t1)
        text_ref(h_t1)
    else:
        text("Project")
    end()
    if project != 0:
        jstr(project, "repo_url", h_t1)
        span("muted mono")
        text_ref(h_t1)
        end()
    end()
    div("body")
    thread_list()
    el("main")
    cls("main")
    pane()
    request_box()
    end()
    end()
    reply_button()
    end()


def view() -> None:
    if screen == 1:
        project_window()
    else:
        project_list()
