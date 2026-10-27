# The Android app

The app is the orchestrator on a phone, the way the desktop window is on a
computer: the ultra UI in a WebView, with the control plane running inside
the app's own process. There is nothing else to run or connect to.

While the control plane runs, the app shows a notification, **The control
plane is running**. That is what keeps Android from freezing the app when
it is off screen: binding a worker installs the runner on a sprite for
minutes, and turns run as long as they take, over connections the control
plane holds, so it keeps running (and keeps the CPU awake) until you stop
it: leaving the app (Back out of it, or swipe it away) or **Stop** in the
notification stops it, as closing the desktop window does. Switching to
another app or turning the screen off does not.

## Install

Download [`orchestrator.apk`](https://github.com/nandi-conda/orchestrate/releases/download/rolling/orchestrator.apk)
from the [rolling release](https://github.com/nandi-conda/orchestrate/releases/tag/rolling)
and open it on the phone (allow installs from your browser when Android asks).
It needs an arm64 phone with Android 13 or newer. Each push to `main` that
changes the app, the control plane or the UI publishes a new build at the same
link, and installing it updates the app in place, keeping its data.

## Keys

On first start the app asks for the control plane's keys:

| Key | What for | How it is sent |
| --- | --- | --- |
| `DEEPSEEK_API_KEY` | the router and every worker | `Authorization: Bearer <key>` to `https://api.deepseek.com` |
| `SPRITES_TOKEN` | workers, which always run on Fly Sprites from the phone | `Authorization: Bearer <token>` to `https://api.sprites.dev/v1` |
| `GITHUB_TOKEN` (optional) | projects with a GitHub remote: fine-grained, Contents: Read and write | `Authorization: Basic base64(x-access-token:<token>)` to GitHub, by git |

They are encrypted with a key that the Android Keystore holds, kept in the
app's private storage, and given to the control plane as environment
variables, where it looks first ([Credentials and configuration](configuration.md)).
Change them from the menu, **Keys**; saving restarts the control plane.

## What runs on the phone

- **Control plane**: the `control` package from `orchestrator/`, unchanged,
  on CPython 3.12 ([Chaquopy](https://chaquo.com/chaquopy/)), serving
  `http://127.0.0.1:47800` to the app's WebView. Every app on a phone can reach
  127.0.0.1, so each request must carry a cookie with a secret made at each
  start, which only the WebView and the app's downloads have.
- **Data**: the SQLite store, project hubs and logs in the app's private
  storage (`~/.orchestrate` there). Uninstalling the app deletes them.
- **git**: built from source for Android (`android/git/build.sh`) with
  curl and OpenSSL, for the hub's worktrees, rebases, bundles and pushes.
- **Workers**: sprites. A phone runs no local workers.

The UI's folder picker browses the app's own storage, since an Android app
sees only that.

## Build it yourself

See [`android/README.md`](https://github.com/nandi-conda/orchestrate/blob/main/android/README.md).
