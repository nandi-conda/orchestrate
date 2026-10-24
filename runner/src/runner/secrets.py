"""Look up secrets: environment first, then the system keyring.

Keyring entries live under the service name "orchestrate" with the env var name
as the username, e.g. `keyring set orchestrate DEEPSEEK_API_KEY` (or
`pixi run set-key DEEPSEEK_API_KEY`).
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
    try:
        import keyring
        return keyring.get_password(SERVICE, name)
    except Exception:  # no keyring backend (headless server), locked keychain, ...
        return None


def get_secret(name: str) -> str | None:
    """Return the value from the environment, else the keyring, else None.

    A value found in the keyring is also put into os.environ so child code and
    subprocesses see it.
    """
    value = os.environ.get(name)
    if value:
        return value
    value = _from_keyring(name)
    if value:
        os.environ[name] = value
    return value


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
    print("\n".join(lines), file=sys.stderr)
    raise SystemExit(1)
