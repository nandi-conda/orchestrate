import argparse
import asyncio
import logging

import uvicorn

from . import paths
from .app import create_app
from .secrets import require, warn_optional
from .server import Server
from .store import store_from_url


def main() -> None:
    p = argparse.ArgumentParser(description="orchestrator control plane")
    p.add_argument("--host", default="127.0.0.1",
                   help="0.0.0.0 (or a tunnel) when browsers reach this from another machine")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--db", default=str(paths.DB),
                   help="SQLite path, or a postgresql:// URL for several control planes")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    paths.migrate()
    logging.getLogger("httpx").setLevel(logging.WARNING)  # a line per request to every worker otherwise
    require(["DEEPSEEK_API_KEY"])  # the router and every worker's model
    warn_optional(["SPRITES_TOKEN", "GITHUB_TOKEN"])
    # EventSource clients hold /events open for as long as their tab is open.
    # The shutdown event ends those streams; the timeout is the backstop for
    # anything else still running.
    shutdown_event = asyncio.Event()
    config = uvicorn.Config(create_app(store=store_from_url(a.db),
                                       shutdown_event=shutdown_event),
                            host=a.host, port=a.port, log_level="info",
                            timeout_graceful_shutdown=5)
    try:
        Server(config, shutdown_event).run()
    except KeyboardInterrupt:  # uvicorn.run() normally swallows this; Server.run() does not
        pass


main()
