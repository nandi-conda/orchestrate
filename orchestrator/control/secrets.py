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

# name -> (what it is for, how it is sent)
KNOWN = {
    "DEEPSEEK_API_KEY": ("DeepSeek API key for the router and local runners",
                         "Authorization: Bearer <key> to https://api.deepseek.com"),
    "SPRITES_TOKEN": ("Fly Sprites org token, needed with WORKER_PROVIDER=sprites",
                      "Authorization: Bearer <token> to https://api.sprites.dev/v1"),
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
    return value


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
              f"Keyring:      pixi run set-key NAME     (stores it under service '{SERVICE}')"]
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
