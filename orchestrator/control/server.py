"""uvicorn for the control plane, shared by `python -m control` and the
desktop window, which runs the control plane in its own process."""

from __future__ import annotations

import asyncio

import uvicorn


class Server(uvicorn.Server):
    """End the SSE streams before uvicorn waits for their connections."""

    def __init__(self, config: uvicorn.Config, shutdown_event: asyncio.Event) -> None:
        super().__init__(config)
        self._shutdown_event = shutdown_event

    async def shutdown(self, sockets=None) -> None:
        self._shutdown_event.set()
        await super().shutdown(sockets)
