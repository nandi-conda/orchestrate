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
                   on_click, on_enter, on_escape, on_input, value_ref, after, s_new, s_set, s_add, s_add_ref, s_add_num,
                   s_clear, s_len, s_eq,
                   s_copy, http_get_ref, j_ok, j_err, j_at, j_len, j_idx, j_str, j_int, j_has, j_free, div, span)
from ui import (btn, btn_open, card_open, card_header, card_content_open, card_footer_open,
                input_open, label, badge, dialog_open, dialog_footer_open, alert, separator,
                toggle_group_open, toggle_open, toggle, tabs_list_open, tab, aria)
from orch import (o_new, o_str, o_lit, o_images, http_post, http_get_text, sse_open, sse_close, page_project,
                  nav_push, on_nav, set_title, scroll_end, scroll_follow, md_parse, md_op, md_str, s_add_enc,
                  clip_copy, on_select, sel_clear, s_quote, o_int, jump_to, j_when, focus_end, is_md, is_img, path_join, path_dir,
                  on_paste, img_count, img_url,
                  img_drop, img_stash, img_restore, img_clear, on_zoom, newline_keys, j_put_row,
                  host_folders, host_pick_folder, code_put, code_text)

# ---- Msg
GOT_PROJECTS: Const[i32] = 1
OPEN: Const[i32] = 2
NEW_SHOW: Const[i32] = 3
NEW_HIDE: Const[i32] = 4
NAME_IN: Const[i32] = 5
REPO_IN: Const[i32] = 6        # the settings' remote field
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
EDIT: Const[i32] = 49
EDIT_IN: Const[i32] = 50
EDIT_SAVE: Const[i32] = 51
EDIT_CANCEL: Const[i32] = 52
EDITED: Const[i32] = 53
SETTINGS_SHOW: Const[i32] = 54
SETTINGS_HIDE: Const[i32] = 55
SET_NAME_IN: Const[i32] = 56
SET_MODEL: Const[i32] = 57      # arg: the model's index (model_id)
SETTINGS_SAVE: Const[i32] = 58
SETTINGS_SAVED: Const[i32] = 59
PASTED: Const[i32] = 60
IMG_DROP: Const[i32] = 61
PROJ_SHOW_ARCHIVED: Const[i32] = 62
PROJ_ARCHIVE: Const[i32] = 63
PROJ_UNARCHIVE: Const[i32] = 64
PROJ_ARCHIVED: Const[i32] = 65
RESPAWN: Const[i32] = 66
RESPAWNED: Const[i32] = 67
THREADS_REFRESH: Const[i32] = 68
FOLDER_IN: Const[i32] = 69
PICK_SHOW: Const[i32] = 70     # arg: the field picked for (PICK_NEW, PICK_REMOTE)
PICK_HIDE: Const[i32] = 71
GOT_FOLDERS: Const[i32] = 72
PICK_OPEN: Const[i32] = 73     # arg: a folder of the listing; -1 its parent; -2 the home folder
PICK_USE: Const[i32] = 74
PICKED: Const[i32] = 75       # arg: the field the desktop window's folder dialog picked for
GOT_PLACES: Const[i32] = 76
THREADS_TOGGLE: Const[i32] = 77
JUMP: Const[i32] = 78         # arg: seq of your message whose quote was clicked
JUMP_MISS: Const[i32] = 79
TAKE_OVER: Const[i32] = 80    # drive the open project from this computer
TOOK_OVER: Const[i32] = 81
APP_SHOW: Const[i32] = 82     # the app's Settings, from the project list
APP_HIDE: Const[i32] = 83
GOT_APP: Const[i32] = 84
KEY_EDIT: Const[i32] = 85     # arg: the key's index; -1 closes its field
KEY_IN: Const[i32] = 86
KEY_SAVE: Const[i32] = 87
KEY_SAVED: Const[i32] = 88
SPACE_COPY: Const[i32] = 89   # copy this computer's sync space key, to pair another computer
COPY_CODE: Const[i32] = 90    # arg: the code's id (code_put)
CODE_COPIED: Const[i32] = 91
CODE_COPIED_DONE: Const[i32] = 92

# pane tabs
DIALOG: Const[i32] = 0
TOOLS: Const[i32] = 1
WLOG: Const[i32] = 2
FILES: Const[i32] = 3
THINKING: Const[i32] = 4

# the fields the folder picker fills
PICK_NEW: Const[i32] = 1       # the Open folder dialog's folder
PICK_REMOTE: Const[i32] = 2    # the settings' remote

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
MD_SRC: Const[i32] = 6
MD_ALT: Const[i32] = 7

# ---- Model
screen: i32 = 0          # 0 project list, 1 a project
projects: i32 = 0        # JSON handle: list of projects
places: i32 = 0          # JSON handle: where threads' workers can run ([{id, label, why}], GET /api/places)
project: i32 = 0         # JSON handle: the open project
threads: i32 = 0         # JSON handle: thread directory
transcript: i32 = 0      # JSON handle: selected thread's events
plog: i32 = 0            # JSON handle: project orchestrator
loading: i32 = 0
show_new: i32 = 0        # 1 the New project dialog is up, 2 the Open folder one
picking: i32 = 0         # the field the folder picker is open for (PICK_NEW, PICK_REMOTE), 0 when closed
host_picks: i32 = 0      # 1 in the desktop window: Browse… is the OS's folder dialog, not the picker
folders: i32 = 0         # JSON handle: the folder the picker shows ({path, parent, entries})
folders_loading: i32 = 0
creating: i32 = 0
sending: i32 = 0
mode: i32 = 0
pending: i32 = 0         # a refresh is scheduled
threads_pending: i32 = 0 # a thread-list refresh is scheduled
pane_tab: i32 = 0        # DIALOG, TOOLS, THINKING, FILES or WLOG
stream: i32 = 0          # the open project's event stream, 0 when none
wlog: i32 = 0            # JSON handle: the selected thread's worker log ({text})
wlog_loading: i32 = 0
files: i32 = 0           # JSON handle: a directory of the selected thread's worktree ({path, entries})
files_loading: i32 = 0
fview: i32 = 0           # JSON handle: the markdown file the Files tab shows ({text}); 0 for an image
fview_req: i32 = 0       # the handle its read will land in, 0 when none is pending
show_archived: i32 = 0   # the thread list shows archived threads instead of live ones
threads_open: i32 = 1    # the thread list is unfolded on a phone (a desktop shows it always)
proj_archived: i32 = 0   # the project list shows archived projects instead of live ones
jump: i32 = 0            # the pane's next content was asked for by navigation: open it at the bottom
jump_seq: i32 = 0        # or at this event instead, the one a clicked quote came from (0 = none)
md_quote: i32 = 0        # seq of the message whose first block quote md() links to its source (0 = none)
copied: i32 = -1         # seq of the reply just copied (shows a check), -1 when none
SPACE_COPIED: Const[i32] = -2  # `copied` while the sync space key is the one just copied
code_copied: i32 = 0     # id of the code span or block just copied (shows a check), 0 when none
md_codes: i32 = 0        # code spans and blocks md() has drawn so far in the markdown it is on
selecting: i32 = 0       # text is selected in the pane (the Reply button shows)
editing: i32 = 0         # seq of the message of yours being edited, 0 when none
edit_sent: i32 = 0       # seq of the edit last saved, reopened if the save fails
show_settings: i32 = 0   # the open project's settings dialog is up
saving_settings: i32 = 0
show_app: i32 = 0        # the app's Settings dialog is up
app: i32 = 0             # JSON handle: the app's settings (GET /api/settings)
key_edit: i32 = -1       # index of the key whose field is open, -1 when none
saving_key: i32 = 0

h_pid: i32 = 0           # project id
h_sel: i32 = 0           # selected thread id ("" = activity view)
h_name: i32 = 0
h_repo: i32 = 0          # the settings' remote ("" = none)
h_set_name: i32 = 0      # the settings' name
h_set_model: i32 = 0     # the settings' model ("" = the default)
h_set_err: i32 = 0       # why the settings could not be saved
h_folder: i32 = 0        # the Open folder dialog's folder
h_new_err: i32 = 0       # why the project could not be created
h_pick: i32 = 0          # the folder the picker shows, or is asked for
h_pick_err: i32 = 0      # why the picker's folder could not be listed
h_req: i32 = 0
h_last: i32 = 0          # the request being sent, put back if the send fails
h_err: i32 = 0
h_route: i32 = 0         # last router decision
h_url: i32 = 0
h_t1: i32 = 0
h_t2: i32 = 0
h_t3: i32 = 0
h_when: i32 = 0         # an event's time, local
h_when_full: i32 = 0    # and its full date, time and zone
h_working: i32 = 0
h_idle: i32 = 0
h_failed: i32 = 0
h_queued: i32 = 0
h_stopped: i32 = 0
h_bound: i32 = 0
h_unbound: i32 = 0
h_ready: i32 = 0
h_local: i32 = 0         # "local", the place a thread is badged for
h_place: i32 = 0         # scratch for a place id while finding its label
h_plabel: i32 = 0        # a place's label, for a thread's badge
h_wlog_err: i32 = 0      # why the worker log could not be read
h_fpath: i32 = 0         # the worktree directory the Files tab shows ("" = its root)
h_files_err: i32 = 0     # why the files could not be read
h_fview: i32 = 0         # worktree path of the markdown or image file the Files tab shows ("" = the listing)
h_fview_err: i32 = 0     # why that file could not be read
h_md: i32 = 0            # scratch for markdown text
h_mdtid: i32 = 0         # thread whose worktree the markdown being drawn names images in ("" = none)
h_mdbase: i32 = 0        # and the worktree directory its relative image paths start from
h_mdsrc: i32 = 0         # scratch for an image's worktree path
h_mdkey: i32 = 0         # where the markdown being drawn is shown, so its code keeps its id
h_code: i32 = 0          # scratch for a code span's text
h_ckey: i32 = 0          # and its key: h_mdkey and how many code spans came before it
h_run: i32 = 0           # thread id of the thinking run being grouped
h_tid: i32 = 0           # scratch for a thread id while finding its title
h_clip: i32 = 0          # the text being copied
h_seltext: i32 = 0       # the text selected in the pane
h_selpos: i32 = 0        # style placing the Reply button over the selection
h_quote: i32 = 0         # the selection the next request replies to ("" = none)
h_lastq: i32 = 0         # the quote being sent, put back if the send fails
h_selsrc: i32 = 0        # seq of the event the pane's selection is in ("" = none, e.g. a reply still streaming)
h_qsrc: i32 = 0          # seq of the event the quote came from ("" = none)
h_lastqsrc: i32 = 0      # and of the quote being sent
h_evid: i32 = 0          # scratch for a row's element id
h_act: i32 = 0           # the thread being archived or unarchived
h_body: i32 = 0          # the request's text as sent: the quote, then the reply
h_edit: i32 = 0          # the text of the message being edited
h_img: i32 = 0           # scratch for an image's URL while it is drawn
h_key: i32 = 0           # the value typed for the key being set ("" removes it)
h_key_err: i32 = 0       # why the key could not be kept, or the settings read


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
    global h_bound, h_unbound, h_ready, h_local, h_place, h_plabel
    global h_working, h_idle, h_failed, h_queued, h_stopped, h_wlog_err, h_md, h_run, h_tid, h_fpath, h_files_err
    global h_fview, h_fview_err, h_mdtid, h_mdbase, h_mdsrc, h_mdkey, h_code, h_ckey
    global h_clip, h_seltext, h_selpos, h_quote, h_lastq, h_body, h_act, h_edit
    global h_selsrc, h_qsrc, h_lastqsrc, h_evid
    global h_set_name, h_set_model, h_set_err, h_folder, h_new_err, h_pick, h_pick_err
    global h_img, h_when, h_when_full, host_picks, h_key, h_key_err
    h_pid = s_new()
    h_sel = s_new()
    h_name = s_new()
    h_repo = s_new()
    h_set_name = s_new()
    h_set_model = s_new()
    h_set_err = s_new()
    h_folder = s_new()
    h_new_err = s_new()
    h_pick = s_new()
    h_pick_err = s_new()
    h_req = s_new()
    h_last = s_new()
    h_err = s_new()
    h_route = s_new()
    h_url = s_new()
    h_t1 = s_new()
    h_t2 = s_new()
    h_t3 = s_new()
    h_when = s_new()
    h_when_full = s_new()
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
    h_bound = s_new()
    s_set(h_bound, "bound")
    h_unbound = s_new()
    s_set(h_unbound, "unbound")
    h_ready = s_new()
    s_set(h_ready, "ready")
    h_local = s_new()
    s_set(h_local, "local")
    h_place = s_new()
    h_plabel = s_new()
    h_wlog_err = s_new()
    h_fpath = s_new()
    h_files_err = s_new()
    h_fview = s_new()
    h_fview_err = s_new()
    h_md = s_new()
    h_mdtid = s_new()
    h_mdbase = s_new()
    h_mdsrc = s_new()
    h_mdkey = s_new()
    h_code = s_new()
    h_ckey = s_new()
    h_run = s_new()
    h_tid = s_new()
    h_clip = s_new()
    h_seltext = s_new()
    h_selpos = s_new()
    h_quote = s_new()
    h_lastq = s_new()
    h_selsrc = s_new()
    h_qsrc = s_new()
    h_lastqsrc = s_new()
    h_evid = s_new()
    h_body = s_new()
    h_act = s_new()
    h_edit = s_new()
    h_img = s_new()
    h_key = s_new()
    h_key_err = s_new()
    host_picks = host_folders()
    s_set(h_url, "/api/places")
    get(h_url, GOT_PLACES)
    on_nav(NAV)
    on_select(h_seltext, h_selpos, h_selsrc, SELECTED)
    on_paste(PASTED)
    newline_keys()
    on_zoom()
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
    global stream, project, threads, transcript, plog, wlog, files, mode, pane_tab, sending, show_archived, editing
    global show_settings, saving_settings, threads_open
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
    s_clear(h_qsrc)
    mode = AUTO
    pane_tab = DIALOG
    sending = 0
    show_archived = 0
    threads_open = 1
    editing = 0
    show_settings = 0
    saving_settings = 0


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
    fetch_projects()


def fetch_projects() -> None:
    s_set(h_url, "/api/projects")
    if proj_archived == 1:
        s_add(h_url, "?archived=1")
    get(h_url, GOT_PROJECTS)


def post_project(i: i32, action: str) -> None:
    """POST /api/projects/<id of the project list's card i>/<action>."""
    e: i32
    o: i32
    e = j_idx(projects, i)
    jstr(e, "id", h_t1)
    j_free(e)
    o = o_new()
    s_set(h_url, "/api/projects/")
    s_add_ref(h_url, h_t1)
    s_add(h_url, "/")
    s_add(h_url, action)
    post(h_url, o, PROJ_ARCHIVED)


# ---- requests

def fetch_threads() -> None:
    # The project too: which computer drives it can change with its threads.
    s_set(h_url, "/api/projects/")
    s_add_ref(h_url, h_pid)
    get(h_url, GOT_PROJECT)
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


def open_thread() -> None:
    """Show thread h_sel in the pane, at its newest events."""
    global threads_open, mode, editing, jump, wlog, files
    # The pane takes the room on a phone; its header brings the list back.
    threads_open = 0
    if mode == AUTO and show_archived == 0:
        mode = TO_THREAD
    editing = 0
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


def go_to_quoted(seq: i32) -> None:
    """Show the event the quote in your message seq came from, highlighted: in
    the pane if it is there, else in its thread (or the project's activity,
    for the project's own events), opened at it rather than at the bottom."""
    global jump_seq, pane_tab
    src: i32 = plog
    n: i32 = 0
    i: i32
    e: i32
    of: i32 = 0
    if s_len(h_sel) > 0:
        src = transcript
    if src != 0:
        n = j_len(src, "")
    for i in range(n):
        e = j_idx(src, i)
        if j_int(e, "seq") == seq and j_has(e, "quote_of") == 1:
            of = j_int(e, "quote_of")
            jstr(e, "quote_thread", h_tid)
        j_free(e)
    if of == 0:
        return
    if shows_seq(src, of) == 1:
        jump_to(of, JUMP_MISS)
    elif s_len(h_tid) > 0 and s_eq(h_tid, h_sel) == 0:
        jump_seq = of
        s_copy(h_sel, h_tid)
        pane_tab = DIALOG
        open_thread()
    elif s_len(h_tid) == 0 and s_len(h_sel) > 0:
        jump_seq = of
        show_activity()
    else:
        s_set(h_err, "The quoted message is older than this view shows.")


def shows_seq(src: i32, seq: i32) -> i32:
    """1 if the events src (the transcript or project orchestrator) have this seq."""
    n: i32 = 0
    i: i32
    e: i32
    found: i32 = 0
    if src != 0:
        n = j_len(src, "")
    for i in range(n):
        e = j_idx(src, i)
        if j_int(e, "seq") == seq:
            found = 1
        j_free(e)
    return found


def show_activity() -> None:
    """Select nothing: the pane shows the project's activity."""
    global mode, pane_tab, jump, editing
    s_clear(h_sel)
    editing = 0
    if mode == TO_THREAD:
        mode = AUTO
    if pane_tab == WLOG or pane_tab == FILES or pane_tab == THINKING:
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


def put_live(e: i32) -> None:
    """Grow the pane's row for live row e (a reply still streaming). Only the
    pane that shows this thread's events has one."""
    if s_len(h_sel) > 0:
        jstr(e, "thread_id", h_t1)
        if s_eq(h_sel, h_t1) == 0:
            return  # another thread's reply: this pane does not show it
        j_put_row(transcript, e)
    else:
        j_put_row(plog, e)


def fetch_wlog() -> None:
    global wlog_loading
    if s_len(h_sel) == 0:
        return
    wlog_loading = 1
    s_set(h_url, "/api/threads/")
    s_add_ref(h_url, h_sel)
    s_add(h_url, "/worker-logs")
    get_text(h_url, GOT_WLOG)


def fetch_folders() -> None:
    """The folders in h_pick ("" = the home folder), for the folder picker."""
    global folders_loading
    folders_loading = 1
    s_set(h_url, "/api/folders?path=")
    s_add_enc(h_url, h_pick)
    get(h_url, GOT_FOLDERS)


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
    in project orchestrator) to dst; 1 if it is there."""
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
    global jump, jump_seq
    if jump_seq != 0:
        jump = 0
        jump_to(jump_seq, JUMP_MISS)
        jump_seq = 0
    elif jump == 1:
        jump = 0
        scroll_end("pane")
    else:
        scroll_follow("pane")


def model_id(i: i32, dst: i32) -> None:
    """The model a project runs on, by its index in the settings' choice, in
    dst: control/models.py MODELS, "" being the runner's default (deepseek-flash)."""
    if i == 1:
        s_set(dst, "deepseek-v4-pro")
    elif i == 2:
        s_set(dst, "deepseek/deepseek-v4.1-flash-ultrafast")
    else:
        s_clear(dst)


def place_label(id: i32, dst: i32) -> None:
    """The name the UI gives the place `id` ("This computer", "Sprites") in dst."""
    i: i32
    e: i32
    s_copy(dst, id)
    if places == 0:
        return
    for i in range(j_len(places, "")):
        e = j_idx(places, i)
        jstr(e, "id", h_place)
        if s_eq(h_place, id) == 1:
            jstr(e, "label", dst)
        j_free(e)


def update(msg: i32, arg: i32) -> None:
    global projects, project, places, threads, transcript, plog, loading, show_new, creating, sending
    global mode, pending, threads_pending, pane_tab, wlog, wlog_loading, show_archived, files, files_loading, jump
    global copied, code_copied, selecting, fview_req, editing, edit_sent, show_settings, saving_settings, proj_archived
    global picking, folders, folders_loading, threads_open, show_app, app, key_edit, saving_key
    e: i32 = 0
    k: i32 = 0
    o: i32 = 0
    if msg == GOT_PROJECTS:
        if screen == 0:
            loading = 0
        projects = take(projects, arg)
    elif msg == GOT_PLACES:
        places = take(places, arg)
    elif msg == APP_SHOW:
        show_app = 1
        key_edit = -1
        s_clear(h_key_err)
        s_set(h_url, "/api/settings")
        get(h_url, GOT_APP)
    elif msg == APP_HIDE:
        if saving_key == 0:
            show_app = 0
            key_edit = -1
            s_clear(h_key)
    elif msg == GOT_APP:
        if j_ok(arg) == 1:
            app = take(app, arg)
        else:
            j_err(arg, h_key_err)
            j_free(arg)
    elif msg == KEY_EDIT:
        if saving_key == 0:
            key_edit = arg
            s_clear(h_key)
            s_clear(h_key_err)
    elif msg == KEY_IN:
        s_copy(h_key, arg)
    elif msg == KEY_SAVE:
        if key_edit >= 0 and saving_key == 0 and app != 0:
            saving_key = 1
            s_clear(h_key_err)
            o = o_new()
            key_name(key_edit, h_t1)
            o_str(o, "name", h_t1)
            o_str(o, "value", h_key)
            s_set(h_url, "/api/settings/keys")
            post(h_url, o, KEY_SAVED)
    elif msg == KEY_SAVED:
        saving_key = 0
        if j_ok(arg) == 1:
            app = take(app, arg)
            key_edit = -1
            s_clear(h_key)
        else:
            j_err(arg, h_key_err)
            j_free(arg)
    elif msg == OPEN:
        e = j_idx(projects, arg)
        jstr(e, "id", h_t1)
        j_free(e)
        open_project(h_t1)
    elif msg == PROJ_SHOW_ARCHIVED:
        proj_archived = arg
        loading = 1
        projects = take_none(projects)
        s_clear(h_err)
        fetch_projects()
    elif msg == PROJ_ARCHIVE:
        post_project(arg, "archive")
    elif msg == PROJ_UNARCHIVE:
        post_project(arg, "unarchive")
    elif msg == PROJ_ARCHIVED:
        # Either way the project leaves the list on screen.
        if j_ok(arg) == 1:
            s_clear(h_err)
            if screen == 0:
                fetch_projects()
        else:
            j_err(arg, h_err)
        j_free(arg)
    elif msg == NEW_SHOW:
        # arg 0: a new project; 1: one opened from a folder on disk.
        if creating == 0:
            s_clear(h_new_err)
            show_new = arg + 1
            picking = 0
    elif msg == NEW_HIDE:
        if creating == 0:
            show_new = 0
            picking = 0
    elif msg == NAME_IN:
        s_copy(h_name, arg)
    elif msg == FOLDER_IN:
        s_copy(h_folder, arg)
    elif msg == PICK_SHOW:
        # The picker starts at the folder the field names, the home folder
        # when it names none. The desktop window shows the OS's dialog instead.
        if arg == PICK_NEW:
            s_copy(h_pick, h_folder)
        else:
            s_copy(h_pick, h_repo)
        if host_picks == 1:
            host_pick_folder(h_pick, PICKED, arg)
        else:
            picking = arg
            s_clear(h_pick_err)
            fetch_folders()
    elif msg == PICKED:
        if arg == PICK_NEW:
            s_copy(h_folder, h_pick)
        else:
            s_copy(h_repo, h_pick)
    elif msg == PICK_HIDE:
        picking = 0
    elif msg == PICK_OPEN:
        if arg == -2:
            s_clear(h_pick)
        elif arg == -1:
            jstr(folders, "parent", h_pick)
        else:
            o = j_at(folders, "entries")
            e = j_idx(o, arg)
            jstr(e, "path", h_pick)
            j_free(e)
            j_free(o)
        fetch_folders()
    elif msg == GOT_FOLDERS:
        # A folder that cannot be listed keeps the last listing under its reason.
        folders_loading = 0
        if j_ok(arg) == 1:
            s_clear(h_pick_err)
            folders = take_none(folders)
            folders = arg
        else:
            j_err(arg, h_pick_err)
            j_free(arg)
    elif msg == PICK_USE:
        if folders != 0:
            if picking == PICK_NEW:
                jstr(folders, "path", h_folder)
            else:
                jstr(folders, "path", h_repo)
        picking = 0
    elif msg == CREATE:
        # A folder is joined to the project's repository before the reply comes
        # back, so opening one can take as long as copying its history.
        if creating == 0 and ((show_new == 1 and s_len(h_name) > 0) or (show_new == 2 and s_len(h_folder) > 0)):
            creating = 1
            s_clear(h_new_err)
            o = o_new()
            o_str(o, "name", h_name)
            if show_new == 2:
                o_str(o, "folder", h_folder)
            s_set(h_url, "/api/projects")
            post(h_url, o, CREATED)
    elif msg == CREATED:
        creating = 0
        if j_ok(arg) == 1:
            show_new = 0
            jstr(arg, "id", h_t1)
            s_clear(h_name)
            s_clear(h_folder)
            open_project(h_t1)
        else:
            j_err(arg, h_new_err)
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
        open_thread()
    elif msg == ACTIVITY:
        threads_open = 0
        show_activity()
    elif msg == THREADS_TOGGLE:
        if threads_open == 1:
            threads_open = 0
        else:
            threads_open = 1
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
        # Images are the request too: a pasted screenshot with no words still sends.
        if (s_len(h_req) > 0 or img_count() > 0) and sending == 0:
            sending = 1
            s_copy(h_last, h_req)
            s_copy(h_lastq, h_quote)
            s_copy(h_lastqsrc, h_qsrc)
            s_clear(h_req)
            s_clear(h_quote)
            s_clear(h_qsrc)
            s_clear(h_err)
            # A reply to a selection goes out as markdown: the quote, then the reply.
            s_clear(h_body)
            if s_len(h_lastq) > 0:
                s_quote(h_body, h_lastq)
                s_add(h_body, "\n\n")
            s_add_ref(h_body, h_last)
            o = o_new()
            o_str(o, "text", h_body)
            if s_len(h_lastq) > 0 and s_len(h_lastqsrc) > 0:
                o_int(o, "quote_of", h_lastqsrc)
            if img_count() > 0:
                o_images(o, "images")
                img_stash()
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
                s_copy(h_qsrc, h_lastqsrc)
            img_restore()
        else:
            img_clear()
        j_free(arg)
    elif msg == IMG_DROP:
        img_drop(arg)
    elif msg == COPY:
        # arg is the reply's seq; its raw markdown is what gets copied.
        if find_text(arg, h_clip) == 1:
            clip_copy(h_clip, COPIED, arg, COPY_FAILED)
    elif msg == SPACE_COPY:
        if app != 0:
            jstr(app, "sync.space", h_clip)
            clip_copy(h_clip, COPIED, SPACE_COPIED, COPY_FAILED)
    elif msg == COPY_CODE:
        # arg is a code span's id; its text is what md() last drew for it.
        code_text(arg, h_clip)
        clip_copy(h_clip, CODE_COPIED, arg, COPY_FAILED)
    elif msg == CODE_COPIED:
        code_copied = arg
        after(1500, CODE_COPIED_DONE, arg)
    elif msg == CODE_COPIED_DONE:
        if code_copied == arg:
            code_copied = 0
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
        s_copy(h_qsrc, h_selsrc)
        if s_len(h_sel) > 0 and show_archived == 0:
            mode = TO_THREAD
        sel_clear()
        focus_end("req")
    elif msg == UNQUOTE:
        s_clear(h_quote)
        s_clear(h_qsrc)
        focus_end("req")
    elif msg == JUMP:
        go_to_quoted(arg)
    elif msg == JUMP_MISS:
        s_set(h_err, "The quoted message is older than this view shows.")
    elif msg == EDIT:
        # arg is the seq of one of your messages; it opens for editing in place.
        if find_text(arg, h_edit) == 1:
            editing = arg
    elif msg == EDIT_IN:
        s_copy(h_edit, arg)
    elif msg == EDIT_CANCEL:
        editing = 0
    elif msg == EDIT_SAVE:
        # Like sending: the control plane takes the edit and returns; the
        # message shows its new text once the "edited" event is in, and the
        # router's re-assessment arrives as a "route" event.
        if editing != 0 and s_len(h_edit) > 0:
            o = o_new()
            o_str(o, "text", h_edit)
            s_set(h_url, "/api/projects/")
            s_add_ref(h_url, h_pid)
            s_add(h_url, "/messages/")
            s_add_num(h_url, editing)
            s_add(h_url, "/edit")
            edit_sent = editing
            editing = 0
            post(h_url, o, EDITED)
    elif msg == EDITED:
        if j_ok(arg) == 0:
            j_err(arg, h_err)
            if editing == 0:
                editing = edit_sent
        j_free(arg)
    elif msg == EVENT:
        k = j_int(arg, "kind")
        if k == K_ROUTE:
            jstr(arg, "text", h_route)
        elif k == K_ERROR and j_has(arg, "thread_id") == 0:
            jstr(arg, "text", h_err)
        # A reply still streaming is one row that grows, so the pane it is in
        # patches that row. Its first delta is not: a part starting moves what
        # the thread list says each thread is doing, and nothing in the pane.
        if j_int(arg, "live") == 1:
            if j_int(arg, "began") == 1:
                if threads_pending == 0:
                    threads_pending = 1
                    after(150, THREADS_REFRESH, 0)
            else:
                put_live(arg)
        elif pending == 0:
            pending = 1
            after(150, REFRESH, 0)
        j_free(arg)
    elif msg == THREADS_REFRESH:
        threads_pending = 0
        if screen == 1:
            fetch_threads()
    elif msg == REFRESH:
        pending = 0
        if screen == 1:
            fetch_threads()
            if s_len(h_sel) > 0:
                fetch_transcript()
                # Files the thread could not show yet (it was not on a worker)
                # are asked for again as its binding moves.
                if pane_tab == FILES and files == 0 and files_loading == 0:
                    fetch_files()
            else:
                fetch_log()
    elif msg == STOP:
        if s_len(h_sel) > 0:
            o = o_new()
            s_set(h_url, "/api/threads/")
            s_add_ref(h_url, h_sel)
            s_add(h_url, "/stop")
            post(h_url, o, STOPPED)
    elif msg == RESPAWN:
        if s_len(h_sel) > 0:
            s_copy(h_act, h_sel)
            post_thread(h_act, "respawn", RESPAWNED)
    elif msg == RESPAWNED:
        # The thread comes back in the background; its "binding" events
        # refresh the pane as they land.
        if j_ok(arg) == 0:
            j_err(arg, h_err)
        j_free(arg)
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
        # The list that was just asked for is the one to read.
        threads_open = 1
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
        # arg indexes a markdown or image file of the listing shown, to show
        # in the pane: markdown is read as text and rendered; an image is
        # loaded by the <img> itself.
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
        if is_img(h_t1) == 0:
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
    elif msg == SETTINGS_SHOW:
        # The fields start as the project is; nothing changes until Save.
        if project != 0:
            jstr(project, "name", h_set_name)
            jstr(project, "model", h_set_model)
            jstr(project, "repo_url", h_repo)
            s_clear(h_set_err)
            show_settings = 1
            picking = 0
    elif msg == SETTINGS_HIDE:
        if saving_settings == 0:
            show_settings = 0
            picking = 0
    elif msg == SET_NAME_IN:
        s_copy(h_set_name, arg)
    elif msg == SET_MODEL:
        model_id(arg, h_set_model)
    elif msg == REPO_IN:
        s_copy(h_repo, arg)
    elif msg == SETTINGS_SAVE:
        # A changed remote is connected to the project's repository before the
        # reply comes back, so this can take as long as a fetch.
        if s_len(h_set_name) > 0 and saving_settings == 0:
            saving_settings = 1
            s_clear(h_set_err)
            o = o_new()
            o_str(o, "name", h_set_name)
            o_str(o, "model", h_set_model)
            o_str(o, "repo_url", h_repo)
            s_set(h_url, "/api/projects/")
            s_add_ref(h_url, h_pid)
            s_add(h_url, "/settings")
            post(h_url, o, SETTINGS_SAVED)
    elif msg == SETTINGS_SAVED:
        saving_settings = 0
        if j_ok(arg) == 1:
            show_settings = 0
            project = take(project, arg)
            jstr(project, "name", h_t1)
            set_title(h_t1)
        else:
            j_err(arg, h_set_err)
            j_free(arg)
    elif msg == TAKE_OVER:
        if project != 0:
            o = o_new()
            s_set(h_url, "/api/projects/")
            s_add_ref(h_url, h_pid)
            s_add(h_url, "/drive")
            post(h_url, o, TOOK_OVER)
    elif msg == TOOK_OVER:
        if j_ok(arg) == 1:
            project = take(project, arg)
            fetch_threads()
        else:
            j_free(arg)
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


def thread_badges(t: i32) -> None:
    """Where a thread stands (states.py in the control plane). On a worker:
    its runner's state (queued, working, idle...), and its worker's state when
    that is not ready. On none, or on its way to one: its binding, since a
    runner state is only true while a runner has it; a turn that ended badly
    (failed, stopped) still says so. Then the messages it has not got to, if
    any (its load)."""
    jstr(t, "binding", h_t2)
    if s_eq(h_t2, h_bound) == 1:
        jstr(t, "state", h_t2)
        state_badge(h_t2)
        jstr(t, "worker_state", h_t2)
        if s_len(h_t2) > 0 and s_eq(h_t2, h_ready) == 0:
            outline_badge("worker ", h_t2)
    else:
        outline_badge("", h_t2)
        jstr(t, "state", h_t2)
        if s_eq(h_t2, h_failed) == 1 or s_eq(h_t2, h_stopped) == 1:
            state_badge(h_t2)
    jstr(t, "workers", h_t2)
    if s_eq(h_t2, h_local) == 1:
        place_label(h_t2, h_plabel)
        el("span")
        cls("badge")
        cls("secondary")
        # The router's reason for putting it here, when it gave one.
        jstr(t, "workers_why", h_t3)
        if s_len(h_t3) == 0:
            s_set(h_t3, "Its workers run on this computer; the rest run on Sprites")
        attr_ref("title", h_t3)
        text_ref(h_plabel)
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
    if proj_archived == 1:
        text("Archived projects")
    else:
        text("Projects")
    end()
    div("topbar-actions")
    if proj_archived == 1:
        btn("← Projects", "ghost", PROJ_SHOW_ARCHIVED, 0)
    else:
        btn("Settings", "ghost", APP_SHOW, 0)
        btn("Archived", "ghost", PROJ_SHOW_ARCHIVED, 1)
        btn("Open folder", "outline", NEW_SHOW, 1)
        btn("New project", "", NEW_SHOW, 0)
    end()
    end()
    error_banner()
    if projects != 0:
        n = j_len(projects, "")
    if loading == 1:
        el("p")
        cls("muted")
        text("Loading…")
        end()
    elif n == 0 and proj_archived == 1:
        el("p")
        cls("muted")
        text("No archived projects.")
        end()
    elif n == 0:
        card_open()
        card_header("No projects yet", "A project is a git repository plus the threads working on it.")
        card_footer_open()
        btn("Open a folder", "outline", NEW_SHOW, 1)
        btn("Create a project", "", NEW_SHOW, 0)
        end()
        end()
    div("grid")
    for i in range(n):
        e = j_idx(projects, i)
        jstr(e, "id", h_t3)
        # The card opens the project; the archive button sits beside it rather
        # than inside it, so a click there archives without opening.
        el("div")
        cls("project-item")
        key(i + 1)
        el("div")
        cls("card")
        cls("project-card")
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
            text("no remote")
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
        el("button")
        cls("project-archive")
        if proj_archived == 1:
            on_click(PROJ_UNARCHIVE, i)
            attr("title", "Unarchive")
            attr("aria-label", "Unarchive")
            text("Unarchive")
        else:
            on_click(PROJ_ARCHIVE, i)
            attr("title", "Stops its running threads, destroys its workers and hides it from the list")
            attr("aria-label", "Archive")
            text("Archive")
        end()
        end()
        j_free(e)
    end()
    end()
    if show_new == 1:
        new_project_dialog()
    elif show_new == 2:
        open_folder_dialog()
    if show_app == 1:
        app_dialog()


def key_name(i: i32, dst: i32) -> None:
    """The name of the app settings' key i."""
    ks: i32 = j_at(app, "keys")
    e: i32 = j_idx(ks, i)
    jstr(e, "name", dst)
    j_free(e)
    j_free(ks)


def app_dialog() -> None:
    """This computer's control plane: its router model, data folder, sync
    server, where workers run and the keys it uses, each kept in the keyring
    from here unless the environment sets it."""
    i: i32
    e: i32 = 0
    ks: i32 = 0
    n: i32 = 0
    dialog_open("Settings", "This computer's orchestrator, for every project. "
                "Each project has its own settings too.", APP_HIDE)
    el("div")
    cls("form")
    cls("app-settings")
    if app == 0:
        if s_len(h_key_err) > 0:
            el("div")
            cls("alert")
            cls("destructive")
            attr("role", "alert")
            text_ref(h_key_err)
            end()
        else:
            el("p")
            cls("muted")
            text("Loading…")
            end()
    else:
        label("Router model")
        el("p")
        cls("mono")
        cls("repo-path")
        jstr(app, "router_model", h_t1)
        text_ref(h_t1)
        end()
        label("Data folder")
        el("p")
        cls("mono")
        cls("repo-path")
        jstr(app, "data_dir", h_t1)
        text_ref(h_t1)
        end()
        label("Sync")
        el("p")
        cls("help")
        if j_has(app, "sync.url") == 1:
            text("Projects are shared with your other computers through ")
            span("mono")
            jstr(app, "sync.url", h_t1)
            text_ref(h_t1)
            end()
            text(", in this computer's space. This computer is ")
            jstr(app, "sync.computer", h_t1)
            text_ref(h_t1)
            text(".")
        else:
            text("Off: projects stay on this computer.")
        end()
        if j_has(app, "sync.url") == 1 and j_int(app, "sync.caught_up") == 0:
            el("div")
            cls("alert")
            cls("destructive")
            attr("role", "alert")
            jstr(app, "sync.problem", h_t1)
            if s_len(h_t1) > 0:
                text("Not reached yet, so nothing is shared: ")
                text_ref(h_t1)
            else:
                text("Catching up with the sync server…")
            end()
        if j_has(app, "sync.space") == 1:
            label("Space key")
            el("p")
            cls("help")
            text("To share these projects with another computer, copy this key and set it there as "
                 "ORCHESTRATE_SYNC_SPACE (in its Settings). Anyone with the key can see and change them.")
            end()
            div("settings-row")
            span("mono")
            cls("repo-path")
            jstr(app, "sync.space", h_t1)
            text_ref(h_t1)
            end()
            btn_open("outline", SPACE_COPY, 0)
            cls("sm")
            if copied == SPACE_COPIED:
                text("Copied")
            else:
                text("Copy")
            end()
            end()
        label("Workers")
        div("settings-list")
        if places != 0:
            n = j_len(places, "")
        for i in range(n):
            e = j_idx(places, i)
            div("settings-item")
            div("settings-row")
            span("title")
            jstr(e, "label", h_t1)
            text_ref(h_t1)
            end()
            jstr(e, "why", h_t2)
            if s_len(h_t2) == 0:
                badge("available", "secondary")
            else:
                badge("off", "outline")
            end()
            if s_len(h_t2) > 0:
                el("p")
                cls("help")
                text_ref(h_t2)
                end()
            end()
            j_free(e)
        end()
        label("Keys")
        el("p")
        cls("help")
        jstr(app, "keyring", h_t1)
        if s_len(h_t1) > 0:
            text("Read from the environment first, then the keyring. Keys kept here go to the keyring "
                 "and are used from the app's next start.")
        else:
            text("This computer has no keyring: set keys in the environment the app starts in.")
        end()
        ks = j_at(app, "keys")
        n = j_len(ks, "")
        div("settings-list")
        for i in range(n):
            e = j_idx(ks, i)
            key_row(e, i)
            j_free(e)
        end()
        j_free(ks)
        if key_edit < 0 and s_len(h_key_err) > 0:
            key_error()
    end()
    dialog_footer_open()
    btn("Close", "outline", APP_HIDE, 0)
    end()
    end()


def key_row(e: i32, i: i32) -> None:
    """A key: its name and where it is found, what it is for, and a field
    to keep it in the keyring while it is open."""
    div("settings-item")
    div("settings-row")
    span("mono")
    jstr(e, "name", h_t1)
    text_ref(h_t1)
    end()
    jstr(e, "source", h_t2)
    if s_len(h_t2) == 0:
        badge("not set", "outline")
    else:
        el("span")
        cls("badge")
        cls("secondary")
        text_ref(h_t2)
        end()
    span("grow")
    end()
    # One the environment sets is changed there: it wins over the keyring.
    jstr(app, "keyring", h_t1)
    s_set(h_t3, "environment")
    if key_edit != i and s_len(h_t1) > 0 and s_eq(h_t2, h_t3) == 0:
        btn_open("ghost", KEY_EDIT, i)
        cls("sm")
        if s_len(h_t2) == 0:
            text("Set…")
        else:
            text("Change…")
        end()
    end()
    el("p")
    cls("help")
    jstr(e, "what", h_t1)
    text_ref(h_t1)
    text(". Sent as ")
    jstr(e, "sent", h_t1)
    text_ref(h_t1)
    text(".")
    end()
    if key_edit == i:
        div("field-row")
        input_open("empty removes it from the keyring", h_key, KEY_IN)
        attr("type", "password")
        autofocus()
        on_enter(KEY_SAVE, 0)
        end()
        btn("Cancel", "outline", KEY_EDIT, -1)
        btn_open("", KEY_SAVE, 0)
        if saving_key == 1:
            text("Saving…")
        else:
            text("Save")
        end()
        end()
        if s_len(h_key_err) > 0:
            key_error()
    end()


def key_error() -> None:
    el("div")
    cls("alert")
    cls("destructive")
    attr("role", "alert")
    text_ref(h_key_err)
    end()


def new_project_dialog() -> None:
    dialog_open("New project", "It starts with its own git repository on this machine. "
                "A GitHub remote can be attached later, in its settings.", NEW_HIDE)
    el("div")
    cls("form")
    label("Name")
    input_open("my-app", h_name, NAME_IN)
    autofocus()
    on_enter(CREATE, 0)
    end()
    new_project_error()
    end()
    create_footer()


def open_folder_dialog() -> None:
    """A project opened from a folder on the control plane's machine: the
    folder becomes its remote (control/hub.py, `folder`)."""
    dialog_open("Open folder", "A project from a folder on this computer. Finished threads land in "
                "its checked-out branch, so their work shows up in its files.", NEW_HIDE)
    el("div")
    cls("form")
    label("Folder")
    div("field-row")
    input_open("/home/you/my-app", h_folder, FOLDER_IN)
    autofocus()
    on_enter(CREATE, 0)
    end()
    btn("Browse…", "outline", PICK_SHOW, PICK_NEW)
    end()
    if picking == PICK_NEW:
        folder_picker()
    el("p")
    cls("help")
    text("A git repository's folder, or any folder: one that is not a repository becomes one, "
         "its files the first commit. A project that is not a pixi project gets pixi's files, "
         "committed there. Keep your own changes there committed or stashed while threads "
         "finish: git will not land over them.")
    end()
    label("Name (optional)")
    input_open("the folder's name", h_name, NAME_IN)
    on_enter(CREATE, 0)
    end()
    new_project_error()
    end()
    create_footer()


def folder_picker() -> None:
    """Folders on the control plane's machine, one at a time, to fill the
    field the picker was opened for: open a folder to go into it, then use the
    one shown. Git repositories are marked."""
    i: i32
    n: i32 = 0
    e: i32
    ents: i32
    div("picker")
    div("wlog-bar")
    span("mono picker-path")
    if folders != 0:
        jstr(folders, "path", h_t1)
        text_ref(h_t1)
    end()
    div("pane-actions")
    btn_open("ghost", PICK_OPEN, -2)
    cls("sm")
    text("Home")
    end()
    if folders != 0:
        jstr(folders, "parent", h_t1)
        if s_len(h_t1) > 0:
            btn_open("ghost", PICK_OPEN, -1)
            cls("sm")
            text("Up")
            end()
    end()
    end()
    if s_len(h_pick_err) > 0:
        el("div")
        cls("alert")
        cls("destructive")
        attr("role", "alert")
        text_ref(h_pick_err)
        end()
    if folders != 0:
        ents = j_at(folders, "entries")
        n = j_len(ents, "")
        div("files picker-list")
        if n == 0:
            el("p")
            cls("muted")
            cls("file")
            if folders_loading == 1:
                text("Loading…")
            else:
                text("No folders in here.")
            end()
        for i in range(n):
            e = j_idx(ents, i)
            jstr(e, "name", h_t1)
            el("button")
            cls("file")
            key(i + 1)
            on_click(PICK_OPEN, i)
            span("file-name")
            text_ref(h_t1)
            text("/")
            end()
            if j_int(e, "git") == 1:
                badge("git", "secondary")
            end()
            j_free(e)
        end()
        j_free(ents)
    div("picker-actions")
    btn("Cancel", "ghost", PICK_HIDE, 0)
    btn_open("", PICK_USE, 0)
    cls("sm")
    text("Use this folder")
    end()
    end()
    end()


def new_project_error() -> None:
    if s_len(h_new_err) > 0:
        el("div")
        cls("alert")
        cls("destructive")
        attr("role", "alert")
        text_ref(h_new_err)
        end()


def create_footer() -> None:
    dialog_footer_open()
    btn("Cancel", "outline", NEW_HIDE, 0)
    btn_open("", CREATE, 0)
    if creating == 1 and show_new == 2:
        text("Opening…")
    elif creating == 1:
        text("Creating…")
    elif show_new == 2:
        text("Open")
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
    cls_if("collapsed", threads_open == 0)
    div("threads-head")
    btn_open("ghost", THREADS_TOGGLE, 0)
    cls("sm")
    cls("threads-toggle")
    aria("aria-expanded", threads_open == 1)
    attr("title", "Show or hide the thread list")
    attr("aria-label", "Show or hide the thread list")
    if threads_open == 1:
        text("▾")
    else:
        text("▸")
    end()
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
    text("Project orchestrator")
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
        # The title has a line of its own and wraps, so a long one reads in
        # full; the badges go on the line under it.
        div("thread-label")
        jstr(e, "title", h_t1)
        text_ref(h_t1)
        end()
        div("thread-badges")
        thread_badges(e)
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


def peer_label(e: i32) -> None:
    """A message to a thread from another one (or the router), shown once in
    every pane: sender → recipient, a thread by its title."""
    jstr(e, "from", h_t3)
    if thread_index(h_t3) >= 0:
        thread_link(h_t3)
    else:
        text_ref(h_t3)
    el("span")
    cls("ev-arrow")
    text("→")
    end()
    jstr(e, "thread_id", h_t2)
    thread_link(h_t2)


def thread_tag(e: i32, show_thread: i32) -> None:
    if show_thread == 1:
        if j_str(e, "thread_id", h_t2) == 1:
            thread_link(h_t2)


def event_time(e: i32) -> None:
    """When the event happened, in the viewer's local time."""
    if j_when(e, "created_at", h_when, h_when_full) == 1:
        el("time")
        cls("ev-time")
        attr_ref("title", h_when_full)
        text_ref(h_when)
        end()


def working_now() -> None:
    """Project orchestrator's live line per working thread: its title and what it
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


def event_images(e: i32) -> None:
    """The images a message carried, from /api/images/<id>. The log names them
    by id; the bytes come from the control plane once and the browser keeps them."""
    n: i32
    i: i32
    imgs: i32
    one: i32
    n = j_len(e, "images")
    if n == 0:
        return
    imgs = j_at(e, "images")
    div("ev-images")
    i = 0
    while i < n:
        one = j_idx(imgs, i)
        jstr(one, "id", h_t1)
        s_set(h_img, "/api/images/")
        s_add_ref(h_img, h_t1)
        el("img")
        cls("ev-image")
        cls("zoom")
        attr("loading", "lazy")
        attr("alt", "attached image")
        attr("title", "Click to enlarge")
        attr_ref("src", h_img)
        end()
        j_free(one)
        i = i + 1
    end()
    j_free(imgs)


def event_row(e: i32, show_thread: i32) -> None:
    """One dialog entry. While the model streams, its reasoning and reply show
    as they grow; finished reasoning is grouped by thinking_group."""
    global md_quote
    k: i32
    live: i32
    k = j_int(e, "kind")
    live = j_int(e, "live")
    el("div")
    cls("ev")
    key(j_int(e, "seq"))
    # a quote's way back to the row it was selected from (jump_to)
    s_set(h_evid, "ev-")
    s_add_num(h_evid, j_int(e, "seq"))
    attr_ref("id", h_evid)
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
    cls_if("ev-editing", editing != 0 and editing == j_int(e, "seq"))
    div("ev-label")
    if k == K_PEER:
        peer_label(e)
    else:
        jstr(e, "label", h_t1)
        text_ref(h_t1)
        thread_tag(e, show_thread)
    event_time(e)
    end()
    event_images(e)
    jstr(e, "text", h_t1)
    if editing != 0 and editing == j_int(e, "seq"):
        edit_box()
    elif k == K_ASSISTANT or k == K_PEER or k == K_RESULT or k == K_USER:
        # Replies get a copy button and your own messages an edit button; your
        # messages render as markdown too, so a reply to a selection shows its
        # quote as a quote.
        if live == 0 and k != K_USER:
            copy_button(j_int(e, "seq"))
        if j_int(e, "editable") == 1:
            edit_button(j_int(e, "seq"))
        # an image in a reply names a file of the thread that wrote it
        s_clear(h_mdtid)
        j_str(e, "thread_id", h_mdtid)
        if s_len(h_mdtid) == 0:
            s_copy(h_mdtid, h_sel)
        s_clear(h_mdbase)
        el("div")
        cls("ev-text")
        cls("md")
        if k == K_USER and j_has(e, "quote_of") == 1:
            md_quote = j_int(e, "seq")
        s_set(h_mdkey, "ev-")
        s_add_num(h_mdkey, j_int(e, "seq"))
        md(h_t1)
        md_quote = 0
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


def edit_button(seq: i32) -> None:
    """Your message's edit icon: it opens the message for editing in place."""
    btn_open("ghost", EDIT, seq)
    cls("icon")
    cls("sm")
    cls("ev-edit")
    attr("title", "Edit")
    attr("aria-label", "Edit")
    span("icon icon-pencil")
    end()
    end()


def edit_box() -> None:
    """Your message open for editing: Save has the router re-assess it, and
    the threads it reached are told what changed. Escape or Cancel keeps it as it was."""
    el("textarea")
    cls("input")
    cls("edit-box")
    attr("id", "edit-box")
    attr("rows", "3")
    value_ref(h_edit)
    on_input(EDIT_IN)
    on_escape(EDIT_CANCEL, 0)
    autofocus()
    end()
    div("edit-actions")
    span("muted edit-hint")
    text("Threads already on it get the edit as a correction; the router re-assesses the rest.")
    end()
    btn_open("ghost", EDIT_CANCEL, 0)
    cls("sm")
    text("Cancel")
    end()
    btn_open("", EDIT_SAVE, 0)
    cls("sm")
    text("Save")
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
    parsed whole on each render, so it reads right at every step. Images name
    files in thread h_mdtid's worktree, relative to its directory h_mdbase.
    Code copies in one click: a block by its copy button, a span by clicking
    it, unless the span is inside something a click already does (a link, or a
    quote that jumps to its source). h_mdkey says where the markdown is shown."""
    global md_codes
    n: i32
    i: i32
    op: i32
    depth: i32 = 0     # elements open
    clicks: i32 = 0    # depth of the open link or jumping quote, 0 when none
    block: i32 = 0     # the next code opens inside a code block's pre
    md_codes = 0
    n = md_parse(h)
    for i in range(n):
        op = md_op(i)
        if op == MD_END:
            if depth == clicks:
                clicks = 0
            depth -= 1
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
        elif op == MD_SRC:
            # A worktree path: served from the thread's files, from the
            # markdown's own directory unless it starts at the root ("/").
            if s_len(h_mdtid) > 0:
                md_str(i, h_md)
                path_join(h_mdsrc, h_mdbase, h_md)
                s_set(h_md, "/api/threads/")
                s_add_ref(h_md, h_mdtid)
                s_add(h_md, "/files/raw?path=")
                s_add_enc(h_md, h_mdsrc)
                attr_ref("src", h_md)
        elif op == MD_ALT:
            md_str(i, h_md)
            attr_ref("alt", h_md)
        elif op == MD_ALIGN_C:
            cls("center")
        elif op == MD_ALIGN_R:
            cls("right")
        elif op == 20:
            depth += 1
            code_button(code_id(i + 1, n))
            el("pre")
            block = 1
        elif op == 21:
            depth += 1
            el("code")
            if block == 0 and clicks == 0:
                code_click(code_id(i, n))
            block = 0
        else:
            depth += 1
            if op == 33 or (op == 22 and md_quote != 0):
                if clicks == 0:
                    clicks = depth
            md_open(op)


def code_id(i: i32, n: i32) -> i32:
    """The id COPY_CODE copies code op i's text by. It stays the same for the
    same code in the same place while the markdown grows (code_put), so its
    check outlasts a render."""
    global md_codes
    s_clear(h_code)
    if i + 1 < n:
        if md_op(i + 1) == MD_TEXT:
            md_str(i + 1, h_code)
    s_copy(h_ckey, h_mdkey)
    s_add(h_ckey, ":")
    s_add_num(h_ckey, md_codes)
    md_codes += 1
    return code_put(h_ckey, h_code)


def code_button(cid: i32) -> None:
    """A code block's copy icon, a check for a moment after."""
    btn_open("ghost", COPY_CODE, cid)
    cls("icon")
    cls("sm")
    cls("md-copy")
    if code_copied == cid:
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


def code_click(cid: i32) -> None:
    """A code span copies itself when clicked, and turns green for a moment."""
    cls("md-copyable")
    cls_if("copied", code_copied == cid)
    if code_copied == cid:
        attr("title", "Copied")
    else:
        attr("title", "Click to copy")
    on_click(COPY_CODE, cid)


def md_open(op: i32) -> None:
    """Open the element for markdown op code op (TAGS in ui/markdown.js). The
    first block quote of a reply to a selection goes back to where it came from."""
    global md_quote
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
        if md_quote != 0:
            cls("quote-link")
            attr("title", "Go to the quoted message")
            on_click(JUMP, md_quote)
            md_quote = 0
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
    elif op == 38:
        el("img")
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
    (the badges show them). Project orchestrator also leaves out reasoning and
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


def off_happy(t: i32) -> bool:
    """Whether the thread is away from where it should be (states.py in the
    control plane): on no worker, or bound to one that is not ready. One being
    put on a worker is on its way, and says so in its activity."""
    jstr(t, "binding", h_t2)
    if s_eq(h_t2, h_unbound) == 1:
        return True
    jstr(t, "worker_state", h_t2)
    return s_len(h_t2) > 0 and s_eq(h_t2, h_ready) == 0


def outline_badge(prefix: str, h: i32) -> None:
    el("span")
    cls("badge")
    cls("outline")
    text(prefix)
    text_ref(h)
    end()


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
        thread_badges(t)
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
        if t != 0:
            btn_open("ghost", RESPAWN, 0)
            cls("sm")
            attr("title", "Put the thread on a worker afresh, from its branch, files and session; a turn under way is stopped")
            text("Respawn")
            end()
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
    # Why the thread is away from bound on a ready worker, and the way back.
    # Always there too, empty (hidden) while it is where it should be.
    div("where")
    if t != 0 and j_has(t, "archived_at") == 0 and off_happy(t):
        jstr(t, "binding", h_t1)
        span("now-label")
        text_ref(h_t1)
        end()
        span("now-text")
        jstr(t, "binding_reason", h_t1)
        text_ref(h_t1)
        jstr(t, "worker_state", h_t2)
        if s_len(h_t2) > 0 and s_eq(h_t2, h_ready) == 0:
            text(" · worker ")
            text_ref(h_t2)
            text(": ")
            jstr(t, "worker_reason", h_t1)
            text_ref(h_t1)
        end()
        span("muted")
        text("Respawn brings it back")
        end()
    end()


def tab_open(active: bool, arg: i32) -> None:
    el("button")
    cls("tab")
    attr("role", "tab")
    aria("aria-selected", active)
    on_click(TAB, arg)


def pane_tabs(dialog: i32, tools: i32, thoughts: i32, logs: bool) -> None:
    """Dialog (the conversation) and Tools (calls and their output), with
    counts, and for a thread (logs set) Thinking (its reasoning steps), Files
    (its worktree) and Worker log (the runner's stdout and stderr)."""
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
        tab_open(pane_tab == THINKING, THINKING)
        text("Thinking")
        span("tab-count")
        num(thoughts)
        end()
        end()
        tab_open(pane_tab == FILES, FILES)
        text("Files")
        end()
        tab_open(pane_tab == WLOG, WLOG)
        text("Worker log")
        end()
    end()
    end()


def thinking_steps(src: i32, n: i32) -> i32:
    """The Thinking tab: each of the thread's reasoning steps in order, as
    markdown under its local time. A step still streaming is the last one and
    grows as it comes in. Returns how many steps it showed."""
    i: i32
    e: i32
    steps: i32 = 0
    el("ol")
    cls("thinking-steps")
    for i in range(n):
        e = j_idx(src, i)
        if j_int(e, "kind") == K_THINKING:
            steps = steps + 1
            el("li")
            cls("thinking-step")
            cls_if("ev-live", j_int(e, "live") == 1)
            key(j_int(e, "seq"))
            div("step-head")
            span("step-num")
            text("Step ")
            num(steps)
            end()
            event_time(e)
            if j_int(e, "live") == 1:
                span("muted")
                text(" · thinking…")
                end()
            end()
            el("div")
            cls("step-text")
            cls("md")
            jstr(e, "text", h_t1)
            s_copy(h_mdtid, h_sel)
            s_clear(h_mdbase)
            s_set(h_mdkey, "ev-")
            s_add_num(h_mdkey, j_int(e, "seq"))
            md(h_t1)
            end()
            end()
        j_free(e)
    end()
    return steps


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
    """The markdown or image file h_fview, shown in the pane, with Back to its
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
    elif is_img(h_fview) == 1:
        div("fview-img")
        el("img")
        attr_ref("src", h_t2)
        attr_ref("alt", h_fview)
        end()
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
        s_copy(h_mdtid, h_sel)
        path_dir(h_mdbase, h_fview)
        s_set(h_mdkey, "file:")
        s_add_ref(h_mdkey, h_fview)
        md(h_t1)
        end()


def file_list() -> None:
    """The selected thread's worktree, one directory at a time, read from the
    worker that holds it. A directory opens in place; a markdown file opens
    rendered in the pane and an image opens shown there (file_view); any other
    file is a download link (the browser saves it; the page stays)."""
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
        elif is_md(h_t1) == 1 or is_img(h_t1) == 1:
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
    thoughts: i32 = 0
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
        if j_int(e, "kind") == K_THINKING:
            thoughts = thoughts + 1
        if hidden(e, activity) == 0:
            said = said + 1
        j_free(e)
    el("section")
    cls("pane")
    if activity == 1:
        div("pane-head")
        span("title")
        text("Project orchestrator")
        end()
        end()
    else:
        t = selected_thread()
        thread_head(t)
        if t != 0:
            j_free(t)
    pane_tabs(said, tools, thoughts, activity == 0)
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
    if pane_tab == THINKING:
        if thinking_steps(src, n) == 0:
            el("p")
            cls("muted")
            cls("empty")
            text("No thinking steps yet.")
            end()
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


def attachments() -> None:
    """Images pasted into the request box, each with a button to drop it."""
    n: i32
    i: i32
    n = img_count()
    if n == 0:
        return
    div("attachments")
    i = 0
    while i < n:
        div("attachment")
        img_url(i, h_img)
        el("img")
        cls("attach-img")
        cls("zoom")
        attr("alt", "attached image")
        attr("title", "Click to enlarge")
        attr_ref("src", h_img)
        end()
        btn_open("ghost", IMG_DROP, i)
        cls("icon")
        cls("sm")
        cls("attach-x")
        attr("title", "Remove this image")
        attr("aria-label", "Remove this image")
        span("icon icon-x")
        end()
        end()
        end()
        i = i + 1
    end()


def request_box() -> None:
    el("footer")
    cls("composer")
    # Project orchestrator shows the router's decisions in its log; with a thread
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
    attachments()
    div("composer-row")
    toggle_group_open()
    toggle("Router decides", mode == AUTO, MODE, AUTO)
    if s_len(h_sel) > 0 and show_archived == 0:
        toggle("This thread", mode == TO_THREAD, MODE, TO_THREAD)
    toggle("New thread", mode == NEW_THREAD, MODE, NEW_THREAD)
    end()
    end()
    div("composer-row")
    el("textarea")
    cls("input")
    cls("grow")
    cls("composer-box")
    attr("id", "req")
    attr("rows", "1")
    if s_len(h_quote) > 0:
        attr("placeholder", "Reply to the quote…")
    elif img_count() > 0:
        attr("placeholder", "Say what the image is about (optional)…")
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
    # value + events for the request box; Enter sends, Shift+Enter adds a line
    # (newline_keys)
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
    if project != 0 and j_has(project, "archived_at") == 1:
        el("span")
        cls("badge")
        cls("secondary")
        attr("title", "Unarchive it from the project list to use it")
        text("Archived")
        end()
    span("muted mono")
    if project != 0:
        jstr(project, "repo_url", h_t1)
        if s_len(h_t1) > 0:
            text_ref(h_t1)
        else:
            text("no remote")
    end()
    if project != 0:
        # Projects shared with other computers: one drives each (runs its threads).
        jstr(project, "driver_name", h_t1)
        if s_len(h_t1) > 0 and j_int(project, "driven_here") == 0:
            span("muted")
            text("Driven by ")
            text_ref(h_t1)
            end()
            btn_open("ghost", TAKE_OVER, 0)
            cls("sm")
            attr("title", "Drive this project from this computer: its threads move to this computer's workers")
            text("Take over")
            end()

    span("grow")
    end()
    btn_open("ghost", SETTINGS_SHOW, 0)
    cls("sm")
    text("Settings")
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
    if show_settings == 1:
        settings_dialog()
    end()


def model_choice(i: i32, name: str, via: str) -> None:
    """One model in the settings' picker: its name over who serves it."""
    model_id(i, h_t3)
    toggle_open(s_eq(h_set_model, h_t3) == 1, SET_MODEL, i)
    span("model-name")
    text(name)
    end()
    span("model-via")
    text(via)
    end()
    end()


def settings_dialog() -> None:
    """The open project's name, model and remote, and where its repository is."""
    dialog_open("Project settings", "", SETTINGS_HIDE)
    el("div")
    cls("form")
    cls("settings-form")
    div("setting")
    label("Name")
    input_open("my-app", h_set_name, SET_NAME_IN)
    autofocus()
    on_enter(SETTINGS_SAVE, 0)
    end()
    end()
    div("setting")
    label("Model")
    toggle_group_open()
    cls("model-pick")
    model_choice(0, "Flash", "DeepSeek")
    model_choice(1, "V4 Pro", "DeepSeek")
    model_choice(2, "V4.1 UltraFast", "OpenGateway")
    end()
    el("p")
    cls("help")
    text("What this project's threads run on. OpenGateway needs OPENGATEWAY_API_TOKEN in Keys.")
    end()
    end()
    div("setting")
    label("Remote (optional)")
    div("field-row")
    input_open("https://github.com/you/repo or /home/you/my-app", h_repo, REPO_IN)
    on_enter(SETTINGS_SAVE, 0)
    end()
    btn("Browse…", "outline", PICK_SHOW, PICK_REMOTE)
    end()
    if picking == PICK_REMOTE:
        folder_picker()
    el("p")
    cls("help")
    text("Finished threads push to its default branch. Use an empty repository, one that shares "
         "this project's history, or a folder on this computer. Clear it to keep work here only.")
    end()
    end()
    div("setting")
    label("Local repository")
    el("p")
    cls("mono")
    cls("repo-path")
    if project != 0:
        jstr(project, "repo_path", h_t1)
        text_ref(h_t1)
    end()
    end()
    if s_len(h_set_err) > 0:
        el("div")
        cls("alert")
        cls("destructive")
        attr("role", "alert")
        text_ref(h_set_err)
        end()
    end()
    dialog_footer_open()
    btn("Cancel", "outline", SETTINGS_HIDE, 0)
    btn_open("", SETTINGS_SAVE, 0)
    if saving_settings == 1:
        text("Saving…")
    else:
        text("Save")
    end()
    end()
    end()


def view() -> None:
    if screen == 1:
        project_window()
    else:
        project_list()
