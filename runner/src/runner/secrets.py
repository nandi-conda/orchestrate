"""Look up secrets: environment first, then the system keyring.

Keyring entries live under the service name "orchestrate" with the env var name
as the username, e.g. `keyring set orchestrate DEEPSEEK_API_KEY` (or
`pixi run set-key DEEPSEEK_API_KEY`).

An item added by hand in a keyring GUI (Seahorse's Passwords and Keys, KWallet)
carries no service/username attributes, only a label. Those are accepted too
when the label is exactly the variable name.
"""

from __future__ import annotations

import os
import sys

SERVICE = "orchestrate"

# name -> (what it is for, how it is sent)
KNOWN = {
    "DEEPSEEK_API_KEY": ("DeepSeek API key", "Authorization: Bearer <key> to https://api.deepseek.com"),
    "RUNNER_TOKEN": ("token the orchestrator uses to call this runner", "Authorization: Bearer <token> on every route but /health"),
    "ORCHESTRATOR_TOKEN": ("token this runner uses to call the orchestrator peer API", "Authorization: Bearer <token>"),
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
    prompt, and a server must never pop one up in the middle of a request.
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
    """Return the value from the environment, else the keyring, else None.

    A value found in the keyring is also put into os.environ so child code and
    subprocesses see it.
    """
    value = os.environ.get(name)
    if value:
        return value
    value = _from_keyring(name) or _from_keyring_label(name)
    if value:
        os.environ[name] = value
    return value


def keyring_backend() -> str | None:
    """Which backend a lookup talks to, or None when there is none. Missing
    required secrets are the usual symptom, so the message names the backend
    that failed to find them (a `fail` backend means no keyring in this
    session)."""
    try:
        import keyring
        from keyring.backends.fail import Keyring as NoKeyring

        backend = keyring.get_keyring()
    except Exception:
        return None
    if isinstance(backend, NoKeyring):
        return None
    return f"{type(backend).__module__}.{type(backend).__qualname__}"


def require(names: list[str]) -> None:
    """Resolve each secret; if any is missing, print what is needed and exit."""
    missing = [n for n in names if not get_secret(n)]
    if not missing:
        return
    lines = ["Missing required secrets. Set each one as an environment variable or in the keyring:", ""]
    for n in missing:
        what, sent = KNOWN.get(n, ("", ""))
        lines.append(f"  {n}" + (f"  ({what}; sent as {sent})" if what else ""))
    lines += [
        "",
        "Environment:  export NAME=value",
        f"Keyring:      pixi run set-key NAME     (stores it under service '{SERVICE}')",
    ]
    backend = keyring_backend()
    lines.append(f"              keyring backend: {backend}" if backend
                 else "              no keyring backend in this session; use the environment")
    print("\n".join(lines), file=sys.stderr)
    raise SystemExit(1)
