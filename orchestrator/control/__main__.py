import argparse
import logging
import os

import uvicorn

from .app import create_app
from .secrets import require, warn_optional


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
    uvicorn.run(create_app(), host=a.host, port=a.port, log_level="info")


main()
