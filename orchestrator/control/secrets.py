"""Look up secrets: environment first, then the system keyring.

Same convention as runner/: keyring service "orchestrate", username = the env
var name, so one entry serves both projects. Store one with
`pixi run set-key DEEPSEEK_API_KEY`.
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
    try:
        import keyring
        return keyring.get_password(SERVICE, name)
    except Exception:  # no keyring backend (headless server), locked keychain, ...
        return None


def get_secret(name: str) -> str | None:
    """Environment, else keyring, else None. Keyring hits are copied into
    os.environ so runner subprocesses inherit them."""
    value = os.environ.get(name)
    if value:
        return value
    value = _from_keyring(name)
    if value:
        os.environ[name] = value
    return value


def missing_message(names: list[str]) -> str:
    lines = ["Missing secrets. Set each one as an environment variable or in the keyring:", ""]
    for n in names:
        what, sent = KNOWN.get(n, ("", ""))
        lines.append(f"  {n}" + (f"  ({what}; sent as {sent})" if what else ""))
    lines += ["", "Environment:  export NAME=value",
              f"Keyring:      pixi run set-key NAME     (stores it under service '{SERVICE}')"]
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
