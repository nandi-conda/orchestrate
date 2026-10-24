import argparse
import asyncio
import logging

import uvicorn

from .app import create_app
from .secrets import warn_optional
from .store import store_from_url


class Server(uvicorn.Server):
    """End the SSE streams before uvicorn waits for their connections."""

    def __init__(self, config: uvicorn.Config, shutdown_event: asyncio.Event) -> None:
        super().__init__(config)
        self._shutdown_event = shutdown_event

    async def shutdown(self, sockets=None) -> None:
        self._shutdown_event.set()
        await super().shutdown(sockets)


def main() -> None:
    p = argparse.ArgumentParser(description="orchestrator control plane")
    p.add_argument("--host", default="127.0.0.1",
                   help="0.0.0.0 (or a tunnel) when browsers reach this from another machine")
    p.add_argument("--port", type=int, default=8000)
    p.add_argument("--db", default="orchestrator.db",
                   help="SQLite path, or a postgresql:// URL for several control planes")
    p.add_argument("--public-url", default="",
                   help="address a sprite calls back on; needed when the browser reaches "
                        "this control plane on localhost (use the tunnel or public URL)")
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    warn_optional(["DEEPSEEK_API_KEY", "SPRITES_TOKEN"])
    # EventSource clients hold /events open for as long as their tab is open.
    # The shutdown event ends those streams; the timeout is the backstop for
    # anything else still running.
    shutdown_event = asyncio.Event()
    config = uvicorn.Config(create_app(store=store_from_url(a.db), public_url=a.public_url,
                                       shutdown_event=shutdown_event),
                            host=a.host, port=a.port, log_level="info",
                            timeout_graceful_shutdown=5)
    try:
        Server(config, shutdown_event).run()
    except KeyboardInterrupt:  # uvicorn.run() normally swallows this; Server.run() does not
        pass


main()
