import argparse
import asyncio
import logging
import os

import uvicorn

from .app import create_app
from .secrets import require, warn_optional


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
    p.add_argument("--host", default=os.environ.get("ORCHESTRATOR_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.environ.get("ORCHESTRATOR_PORT", "8000")))
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO)
    if os.environ.get("WORKER_PROVIDER") == "sprites":
        require(["SPRITES_TOKEN"])
    warn_optional(["DEEPSEEK_API_KEY"])
    # Local runners call back into the peer API on this address.
    os.environ.setdefault("ORCHESTRATOR_PUBLIC_URL", f"http://127.0.0.1:{a.port}")
    # EventSource clients hold /events open for as long as their tab is open.
    # The shutdown event ends those streams; the timeout is the backstop for
    # anything else still running.
    shutdown_event = asyncio.Event()
    config = uvicorn.Config(create_app(shutdown_event=shutdown_event), host=a.host, port=a.port,
                            log_level="info",
                            timeout_graceful_shutdown=int(os.environ.get("ORCHESTRATOR_GRACEFUL_SHUTDOWN", "5")))
    try:
        Server(config, shutdown_event).run()
    except KeyboardInterrupt:  # uvicorn.run() normally swallows this; Server.run() does not
        pass


main()
