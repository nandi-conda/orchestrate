# Orchestrator UI: a project list at /, and one main window per project at
# /p/<id>. The main window shows the project's threads on the left, the
# selected thread's transcript (or the project's activity) in the middle, and
# a request box at the bottom that goes to the router.
#
# Text lives in ultra's JS string store; this module holds handles. JSON
# replies stay JS-side too, read with j_* by handle.

from lpython import i32, Const
from ultra import (el, end, text, text_ref, num, cls, cls_if, attr, attr_ref, key, autofocus,
                   on_click, on_enter, on_input, value_ref, after, s_new, s_set, s_add, s_add_ref, s_clear, s_len, s_eq,
                   s_copy, http_get_ref, j_ok, j_err, j_len, j_idx, j_str, j_int, j_has, j_free, div, span)
from ui import (btn, btn_open, card_open, card_header, card_content_open, card_footer_open,
                input_open, label, badge, dialog_open, dialog_footer_open, alert, separator,
                toggle_group_open, toggle)
from orch import (o_new, o_str, o_lit, http_post, sse_open, page_project, open_window, go,
                  set_title, scroll_end)

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

# ---- Model
screen: i32 = 0          # 0 project list, 1 project window
projects: i32 = 0        # JSON handle: list of projects
project: i32 = 0         # JSON handle: this window's project
threads: i32 = 0         # JSON handle: thread directory
transcript: i32 = 0      # JSON handle: selected thread's events
plog: i32 = 0            # JSON handle: project activity
loading: i32 = 0
show_new: i32 = 0
creating: i32 = 0
sending: i32 = 0
mode: i32 = 0
pending: i32 = 0         # a refresh is scheduled

h_pid: i32 = 0           # project id
h_sel: i32 = 0           # selected thread id ("" = activity view)
h_name: i32 = 0
h_repo: i32 = 0
h_req: i32 = 0
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


def listen(url: i32, msg: i32) -> None:
    r: i32
    r = sse_open(url, msg)


def init() -> None:
    global h_pid, h_sel, h_name, h_repo, h_req, h_err, h_route, h_url, h_t1, h_t2, h_t3
    global h_working, h_idle, h_failed, h_queued, h_stopped, screen, loading
    h_pid = s_new()
    h_sel = s_new()
    h_name = s_new()
    h_repo = s_new()
    h_req = s_new()
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
    loading = 1
    if page_project(h_pid) == 1:
        screen = 1
        s_set(h_url, "/api/projects/")
        s_add_ref(h_url, h_pid)
        get(h_url, GOT_PROJECT)
        fetch_threads()
        fetch_log()
        s_set(h_url, "/api/projects/")
        s_add_ref(h_url, h_pid)
        s_add(h_url, "/events")
        listen(h_url, EVENT)
    else:
        s_set(h_url, "/api/projects")
        get(h_url, GOT_PROJECTS)


# ---- requests

def fetch_threads() -> None:
    s_set(h_url, "/api/projects/")
    s_add_ref(h_url, h_pid)
    s_add(h_url, "/threads")
    get(h_url, GOT_THREADS)


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

def update(msg: i32, arg: i32) -> None:
    global projects, project, threads, transcript, plog, loading, show_new, creating, sending
    global mode, pending
    e: i32 = 0
    o: i32 = 0
    if msg == GOT_PROJECTS:
        loading = 0
        projects = take(projects, arg)
    elif msg == OPEN:
        e = j_idx(projects, arg)
        jstr(e, "id", h_t1)
        j_free(e)
        s_set(h_url, "/p/")
        s_add_ref(h_url, h_t1)
        s_set(h_t2, "orchestrator-")
        s_add_ref(h_t2, h_t1)
        open_window(h_url, h_t2)
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
            s_set(h_url, "/p/")
            s_add_ref(h_url, h_t1)
            s_set(h_t2, "orchestrator-")
            s_add_ref(h_t2, h_t1)
            open_window(h_url, h_t2)
            s_set(h_url, "/api/projects")
            get(h_url, GOT_PROJECTS)
        else:
            j_err(arg, h_err)
        j_free(arg)
    elif msg == GOT_PROJECT:
        project = take(project, arg)
        if project != 0:
            jstr(project, "name", h_t1)
            set_title(h_t1)
    elif msg == GOT_THREADS:
        loading = 0
        threads = take(threads, arg)
    elif msg == GOT_LOG:
        plog = take(plog, arg)
        if s_len(h_sel) == 0:
            scroll_end("pane")
    elif msg == SELECT:
        e = j_idx(threads, arg)
        jstr(e, "id", h_sel)
        j_free(e)
        if mode == AUTO:
            mode = TO_THREAD
        fetch_transcript()
    elif msg == ACTIVITY:
        s_clear(h_sel)
        if mode == TO_THREAD:
            mode = AUTO
        fetch_log()
    elif msg == GOT_TRANSCRIPT:
        transcript = take(transcript, arg)
        scroll_end("pane")
    elif msg == REQ_IN:
        s_copy(h_req, arg)
    elif msg == MODE:
        mode = arg
    elif msg == SEND:
        if s_len(h_req) > 0 and sending == 0:
            sending = 1
            o = o_new()
            o_str(o, "text", h_req)
            if mode == TO_THREAD and s_len(h_sel) > 0:
                o_lit(o, "mode", "thread")
                o_str(o, "target", h_sel)
            elif mode == NEW_THREAD:
                o_lit(o, "mode", "new")
            else:
                o_lit(o, "mode", "auto")
            s_set(h_url, "/api/projects/")
            s_add_ref(h_url, h_pid)
            s_add(h_url, "/requests")
            post(h_url, o, SENT)
    elif msg == SENT:
        sending = 0
        if j_ok(arg) == 1:
            s_clear(h_req)
            jstr(arg, "summary", h_route)
            if j_str(arg, "why", h_t1) == 1:
                if s_len(h_t1) > 0:
                    s_add(h_route, " — ")
                    s_add_ref(h_route, h_t1)
            # Follow a single new thread so the person sees it start.
            if j_len(arg, "spawned") == 1 and mode != TO_THREAD:
                jstr(arg, "spawned.0.id", h_sel)
                fetch_transcript()
            s_clear(h_err)
        else:
            j_err(arg, h_err)
        j_free(arg)
        fetch_threads()
    elif msg == EVENT:
        j_free(arg)
        if pending == 0:
            pending = 1
            after(150, REFRESH, 0)
    elif msg == REFRESH:
        pending = 0
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
    elif msg == STOPPED:
        j_free(arg)
        fetch_threads()
    elif msg == HOME:
        go("/")


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
        text("Open window")
        end()
        end()
        end()
        j_free(e)
    end()
    end()
    if show_new == 1:
        new_project_dialog()


def new_project_dialog() -> None:
    dialog_open("New project", "Each project gets its own main window.", NEW_HIDE)
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
    text("Threads")
    end()
    if threads != 0:
        n = j_len(threads, "")
    el("span")
    cls("badge")
    cls("secondary")
    num(n)
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
        el("button")
        cls("thread")
        cls_if("active", s_eq(h_t3, h_sel) == 1)
        cls_if("child", j_has(e, "parent_id") == 1)
        key(i + 1)
        on_click(SELECT, i)
        div("thread-top")
        span("thread-title")
        jstr(e, "title", h_t1)
        text_ref(h_t1)
        end()
        jstr(e, "state", h_t2)
        state_badge(h_t2)
        end()
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
        j_free(e)
    end()


def event_row(e: i32, i: i32, show_thread: i32) -> None:
    k: i32
    k = j_int(e, "kind")
    el("div")
    cls("ev")
    key(j_int(e, "seq"))
    cls_if("ev-user", k == K_USER)
    cls_if("ev-assistant", k == K_ASSISTANT)
    cls_if("ev-tool", k == K_TOOL_CALL or k == K_TOOL_RESULT)
    cls_if("ev-status", k == K_STATUS)
    cls_if("ev-peer", k == K_PEER)
    cls_if("ev-error", k == K_ERROR)
    cls_if("ev-route", k == K_ROUTE)
    div("ev-label")
    jstr(e, "label", h_t1)
    text_ref(h_t1)
    if show_thread == 1:
        if j_str(e, "thread_id", h_t2) == 1:
            span("mono muted")
            text(" · ")
            text_ref(h_t2)
            end()
    end()
    el("div")
    cls("ev-text")
    cls_if("mono", k == K_TOOL_CALL or k == K_TOOL_RESULT)
    jstr(e, "text", h_t1)
    text_ref(h_t1)
    end()
    end()


def pane() -> None:
    n: i32 = 0
    i: i32
    e: i32 = 0
    src: i32
    activity: i32
    src = plog
    activity = 1
    if s_len(h_sel) > 0:
        src = transcript
        activity = 0
    el("section")
    cls("pane")
    div("pane-head")
    if activity == 1:
        span("title")
        text("Project activity")
        end()
    else:
        span("title mono")
        text_ref(h_sel)
        end()
        btn_open("ghost", STOP, 0)
        cls("sm")
        text("Stop thread")
        end()
    end()
    el("div")
    cls("events")
    attr("id", "pane")
    if src != 0:
        n = j_len(src, "")
    if n == 0:
        el("p")
        cls("muted")
        cls("empty")
        if activity == 1:
            text("Nothing yet. Type a request below; the router spawns threads for it.")
        else:
            text("Waiting for this thread's first event…")
        end()
    for i in range(n):
        e = j_idx(src, i)
        event_row(e, i, activity)
        j_free(e)
    end()
    end()


def request_box() -> None:
    el("footer")
    cls("composer")
    if s_len(h_route) > 0:
        div("route")
        span("route-label")
        text("Router")
        end()
        text_ref(h_route)
        end()
    error_banner()
    div("composer-row")
    toggle_group_open()
    toggle("Router decides", mode == AUTO, MODE, AUTO)
    if s_len(h_sel) > 0:
        toggle("This thread", mode == TO_THREAD, MODE, TO_THREAD)
    toggle("New thread", mode == NEW_THREAD, MODE, NEW_THREAD)
    end()
    end()
    div("composer-row")
    el("input")
    cls("input")
    cls("grow")
    if mode == TO_THREAD and s_len(h_sel) > 0:
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
    end()


def view() -> None:
    if screen == 1:
        project_window()
    else:
        project_list()
