"""Look up secrets: environment first, then the system keyring.

Same convention as runner/: keyring service "orchestrate", username = the env
var name, so one entry serves both projects. Store one with
`pixi run set-key DEEPSEEK_API_KEY`.

An item added by hand in a keyring GUI (Seahorse's Passwords and Keys, KWallet)
carries no service/username attributes, only a label. Those are accepted too
when the label is exactly the variable name.
"""

from __future__ import annotations

import os
import sys

SERVICE = "orchestrate"

# The names get_secret copied into os.environ from the keyring: their value
# there came from the keyring, not from the environment the app started in.
_COPIED: set[str] = set()

# name -> (what it is for, how it is sent)
KNOWN = {
    "DEEPSEEK_API_KEY": ("DeepSeek API key for the router and every worker",
                         "Authorization: Bearer <key> to https://api.deepseek.com"),
    "OPENGATEWAY_API_TOKEN": ("OpenGateway API key, for projects whose model is "
                              "deepseek/deepseek-v4.1-flash-ultrafast; passed to every worker",
                              "Authorization: Bearer <key> to https://apis.opengateway.ai/v1"),
    "SPRITES_TOKEN": ("Fly Sprites org token, for threads the router puts on Sprites",
                      "Authorization: Bearer <token> to https://api.sprites.dev/v1"),
    "GITHUB_TOKEN": ("GitHub fine-grained token, Contents: Read and write on the project repos; "
                     "the control plane fetches their default branch into each project's hub and "
                     "pushes finished threads' work to it (workers never get it)",
                     'git http.<repo url>.extraHeader "Authorization: Basic base64(x-access-token:<token>)"'),
    "ORCHESTRATE_SYNC_URL": ("the sync server, when it is not the public relay "
                             "https://orchestrate-sync.nandithebull.workers.dev",
                             "the base of every request to the sync server"),
    "ORCHESTRATE_SYNC_SPACE": ("the key of the space to sync projects in, copied from another computer's "
                               "Settings to share its projects; without it this computer has its own space",
                               "X-Sync-Space: <key> to the sync server"),
}


def _from_keyring(name: str) -> str | None:
    """The documented entry: service "orchestrate", username = the name."""
    try:
        import keyring
        return keyring.get_password(SERVICE, name)
    except Exception:  # no keyring backend (headless server), locked keychain, ...
        return None


def _from_keyring_label(name: str) -> str | None:
    """An item added by hand in a keyring GUI, found by its label.

    Looks through the unlocked collections only: a locked one would need a
    prompt, and services must never pop one up in the middle of a request.
    """
    try:
        import secretstorage
    except ImportError:  # not Linux, or a keyring without the Secret Service
        return None
    try:
        bus = secretstorage.dbus_init()
        for collection in secretstorage.get_all_collections(bus):
            if collection.is_locked():
                continue
            for item in collection.get_all_items():
                if item.get_label() == name:
                    return item.get_secret().decode("utf-8", "replace").strip() or None
    except Exception:  # no session bus, item vanished mid-scan, ...
        return None
    return None


def get_secret(name: str) -> str | None:
    """Environment, else keyring, else None. Keyring hits are copied into
    os.environ so runner subprocesses inherit them."""
    value = os.environ.get(name)
    if value:
        return value
    value = _from_keyring(name) or _from_keyring_label(name)
    if value:
        os.environ[name] = value
        _COPIED.add(name)
    return value


def not_found(name: str) -> str:
    """Why get_secret finds no `name`, in a line naming what it looked in:
    the environment, then the keyring (which backend, and what it said when
    it failed), then the unlocked collections' labels (those locked are
    skipped, never prompted for)."""
    looked = [f"{name} is not in the environment"]
    try:
        import keyring
        from keyring.backends.fail import Keyring as NoKeyring

        backend = keyring.get_keyring()
        if isinstance(backend, NoKeyring):
            looked.append("this session has no keyring backend")
        else:
            label = f"{type(backend).__module__}.{type(backend).__qualname__}"
            try:
                keyring.get_password(SERVICE, name)
                looked.append(f'the keyring ({label}) has no entry for service "{SERVICE}", username "{name}"')
            except Exception as e:
                looked.append(f"the keyring ({label}) failed: {type(e).__name__}: {e}")
    except Exception as e:
        looked.append(f"the keyring could not be loaded: {type(e).__name__}: {e}")
    try:
        import secretstorage

        bus = secretstorage.dbus_init()
        locked = [c.get_label() or "unnamed" for c in secretstorage.get_all_collections(bus) if c.is_locked()]
        if locked:
            looked.append(f"locked collections were not searched for an item labeled {name}: {', '.join(locked)}")
    except Exception:
        pass  # not Linux, or no Secret Service: nothing more was looked in
    return "; ".join(looked)


def source(name: str) -> str:
    """Where get_secret finds `name`: "environment", "keyring", or "" when
    neither has it. The environment wins, so a keyring entry under an
    environment variable of the same name is never used."""
    if os.environ.get(name) and name not in _COPIED:
        return "environment"
    if _from_keyring(name) or _from_keyring_label(name):
        return "keyring"
    return ""


def store(name: str, value: str) -> None:
    """Keep `value` in the keyring as `name` (service "orchestrate"), or
    remove the entry when it is empty. What read it at startup (the router,
    the Sprites provider, the sync server) keeps the old value until the next
    start. Raises RuntimeError when there is no keyring backend."""
    import keyring
    from keyring.errors import KeyringError, PasswordDeleteError

    if keyring_backend() is None:
        raise RuntimeError("no keyring backend on this computer; set the key in the environment")
    try:
        if value:
            keyring.set_password(SERVICE, name, value)
        else:
            keyring.delete_password(SERVICE, name)
    except PasswordDeleteError:
        pass  # nothing was kept under the name
    except KeyringError as e:
        raise RuntimeError(f"the keyring ({keyring_backend()}) did not take {name}: {e}") from e


def keyring_backend() -> str | None:
    """Which backend a lookup talks to, or None when there is none. Missing
    secrets are the usual symptom, so the message names the backend that
    failed to find them (a `fail` backend means the session has no keyring)."""
    try:
        import keyring
        from keyring.backends.fail import Keyring as NoKeyring

        backend = keyring.get_keyring()
    except Exception:
        return None
    if isinstance(backend, NoKeyring):
        return None
    return f"{type(backend).__module__}.{type(backend).__qualname__}"


def missing_message(names: list[str]) -> str:
    lines = ["Missing secrets. Set each one as an environment variable or in the keyring:", ""]
    for n in names:
        what, sent = KNOWN.get(n, ("", ""))
        lines.append(f"  {n}" + (f"  ({what}; sent as {sent})" if what else ""))
    lines += ["", "Environment:  export NAME=value",
              f"Keyring:      keyring set {SERVICE} NAME     (`pixi run set-key NAME` in a checkout)"]
    backend = keyring_backend()
    lines.append(f"              keyring backend: {backend}" if backend
                 else "              no keyring backend in this session; use the environment")
    return "\n".join(lines)


def require(names: list[str]) -> None:
    missing = [n for n in names if not get_secret(n)]
    if missing:
        print(missing_message(missing), file=sys.stderr)
        raise SystemExit(1)


def warn_optional(names: list[str]) -> None:
    missing = [n for n in names if not get_secret(n)]
    if missing:
        print(missing_message(missing).replace("Missing secrets.", "Optional secrets not found."), file=sys.stderr)
