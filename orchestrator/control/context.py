"""The address the control plane was reached on, for one request.

Workers are told to call back on this URL (runner/API.md), and where they run
is decided from it: a loopback or private address means the browser is on this
machine or this network, so workers stay local. Only the client knows which
address it dialled, hence the Host header; a proxy in front has to forward
`X-Forwarded-Host` and `X-Forwarded-Proto` for the real address to survive.
"""

from __future__ import annotations

import re
from contextvars import ContextVar

# Set by the app for the duration of a request; empty outside one.
request_base_url: ContextVar[str] = ContextVar("request_base_url", default="")

_HOST = re.compile(r"^(?:[A-Za-z0-9._-]+|\[[0-9A-Fa-f:.]+\])(?::\d{1,5})?$")


def base_url_from_scope(scope: dict) -> str:
    """`scheme://host` for one ASGI request, or "" when the headers are not a
    usable address (a malformed Host must not become a callback URL)."""
    if scope.get("type") != "http":
        return ""
    headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope.get("headers", [])}
    host = (headers.get("x-forwarded-host") or headers.get("host") or "").split(",")[0].strip()
    if not _HOST.match(host):
        return ""
    scheme = (headers.get("x-forwarded-proto") or scope.get("scheme") or "http").split(",")[0]
    scheme = scheme.strip().lower()
    return f"{scheme}://{host}" if scheme in ("http", "https") else ""
