# Using the UI

Everything happens in one window. Projects, threads, files and logs open in
place; nothing opens a new tab or window. Back and Forward move between the
project list and projects without reloading.

## Projects

The start page lists your projects. **New project** asks only for a name; a
project starts with its own local git repository and no remote (cards say
"local only"). **Open folder** makes a project from a folder on the machine
the control plane runs on: type its full path (`~/` works), or **Browse…** to
pick it, and optionally a name (the
folder's name otherwise). The folder becomes the project's remote,
so finished threads land in its checked-out branch and their work shows up in
its files; see [Opening a folder](../architecture/git.md#opening-a-folder).
A folder that cannot be opened is refused with the reason, and no project is
made. Click a card to open the project at `/p/<project id>`;
**← All projects** goes back.

**Browse…** (here and in Settings) is your OS's folder dialog in the
[desktop window](desktop.md). In a browser, and in the Android app, it is the
UI's own folder picker instead: folders from your home folder, one at a time
(click one to go in, **Up** and **Home** to go back, git repositories marked,
hidden folders left out), and **Use this folder** fills the field. A browser
cannot hand a page a folder's path, so the picker lists the control plane's
folders through `GET /api/folders`.

**Settings** in a project's top bar opens a dialog with:

- **Name**, and the **model** its threads use.
- **Remote (optional)**: attach, change or remove a GitHub repository, or a
  folder on this machine (typed, or picked with **Browse…**), at any time. How the two histories are joined is described in
  [Project repos and landing](../architecture/git.md#attaching-a-remote). A
  remote that cannot be joined is refused with git's reason, and nothing
  changes.
- **Local repository**: the path of the project's hub repository on this
  machine, read-only.

Hover a project card for its **Archive** button. Archiving stops every running
thread and takes the project off the shared workers (its repository and
worktrees there), so it holds no compute or disk; its log and hub repository
stay. **Archived** in the top bar lists archived projects, which open
read-only; **Unarchive** brings one back, and its threads return on the pool
with their next message.

## The project window

The left side is the thread list: **Project orchestrator** first, then the
project's threads, each with its state, and an "N waiting" badge when
messages to it are piling up (red once it is backed up: the router then gives
new work to a new thread instead; see
[thread load](../architecture/routing.md#thread-load)). The right side is the pane for what
you selected, with the request box under it.

**Project orchestrator** shows the router's decisions and every thread's events,
newest last (up to the newest 2,000), with a live line per working thread
saying what it is doing.

### Sending a request

Type in the request box and press Enter. The toggles above it decide where it
goes:

| Toggle | What happens |
| --- | --- |
| **Router decides** (default) | The router spawns new threads or messages existing ones, leaning toward spawning. A bulleted or numbered list fans out into one thread per item. |
| **This thread** | Shown when a thread is open: sends the text straight to it. |
| **New thread** | Always starts a fresh thread. |

Enter never waits on the router, a worker or a runner: the box clears at once
and the request shows in the log. The router's decision appears above the box
("routing…" until the model answers) and in Project orchestrator. A new thread
shows up in the list right away, saying "starting on a worker" until its worker
is up (a new sprite can take a while); messages sent to it meanwhile are
delivered in order once it starts.

Spawning never moves you into a thread. You stay where you are; you open a
thread only by clicking it.

### Images

Paste a screenshot or a copied picture into the request box and it becomes an
attachment above the box: up to 4 per message, each up to 8 MB (PNG, JPEG,
WebP or GIF), and a very large screenshot is scaled down in the browser first.
An image-only request skips the router and starts one new thread titled for the
image. Images show in transcripts and open enlarged on a click.

### Replying to a selection, copying

Select text in a pane and a **Reply** button appears over it. The next message
you send goes out as Markdown: the selection quoted, then your text. Every
reply from an agent has a **Copy** button.

### Editing your messages

Your own messages are the blue, right-aligned bubbles. Each has an edit
button. Save, and the bubble shows the new text marked "edited" while the
router re-assesses it in the background:

- Every thread the message reached (the threads its routing spawned or told,
  or the thread you messaged directly) gets a correction from you with both
  versions, and decides what to keep, drop or undo. Nothing is stopped: a
  thread mid-turn reads the correction before its next model call, and a
  finished one comes back and acts on it.
- The router plans the new text knowing those threads, so it starts threads
  only for parts none of them covers. Its decision shows as an "edit:" line in
  Project orchestrator.

## A thread's pane

Open a thread from the list. The pane has a title bar, a **Now** line saying
what the thread is doing ("thinking", "running python_repl: …"), and five
tabs:

| Tab | Shows |
| --- | --- |
| **Dialog** | The conversation: your messages, messages from other threads and the router, and the agent's replies, rendered as Markdown. Replies grow live as the model streams them. Each message shows when it was sent in your local time (hover it for the full date and zone). |
| **Tools** | Every tool call the agent made (`python_repl` code) with its result. |
| **Thinking** | The agent's reasoning steps in order, numbered, each rendered as Markdown under its time in your local time. A step still streaming shows last and grows live. |
| **Files** | The thread's worktree, one directory at a time. Click a directory to open it (**Up** goes back). Markdown files open rendered in the pane and images (png, jpg, gif, webp, svg) open shown there, each with **Back** and **Download**; any other file downloads. Images a markdown file or a reply names by a worktree path (`![plot](out/plot.png)`, relative to the markdown file, or from the root with a leading `/`) show inline; remote images stay links. `.git` is hidden. A thread on no ready worker shows where it stands instead, and its files once it is bound. |
| **Worker log** | Where the thread stands (its binding and its worker's state, each with the reason), the timed steps of its latest binding (and of a worker being made for it), then the last 300 lines its runner wrote, with **Refresh**. The runner logs in UTC; its times show here, and on the control plane's stdout, in the control plane's local time. |

The buttons in the title bar:

- **Stop thread** stops its current turn on the runner.
- **Respawn** brings the thread back on a worker from whatever state it is in.
  When a thread is on no worker, or its worker is not ready, a red bar under
  the title says which state, why, and that Respawn brings it back. See
  [Worker lifecycle](../worker-lifecycle.md).
- **Archive** stops the thread if it is running, then hides it from the
  thread list, the router and the other threads. **Archived** at the top of
  the thread list shows archived threads; **Unarchive** brings one back
  (stopped). An archived thread takes no messages.

Reading a thread's files or sending it a message when it is on no worker puts
it on one first. Nothing is lost by that: its branch, a snapshot of its whole
worktree and its model session come back from the project's repository.
