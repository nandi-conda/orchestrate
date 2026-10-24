"""Run one request locally: `pixi run run "add a README" --repo .`"""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile

from .model import from_spec
from .thread import ThreadHost


def _print(ev: dict):
    d = ev["data"]
    tag = f"[{ev['thread']}]"
    if ev["type"] == "assistant":
        print(f"{tag} {d['content']}")
    elif ev["type"] == "tool_call":
        print(f"{tag} -> {d['name']} {json.dumps(d['arguments'])[:200]}")
    elif ev["type"] == "tool_result":
        print(f"{tag} <- {d['result'][:300]}")
    elif ev["type"] in ("state", "message_in", "error", "created"):
        print(f"{tag} {ev['type']}: {json.dumps(d)[:200]}")


async def amain(args):
    host = ThreadHost(model_factory=from_spec, repo=args.repo,
                      workdir=args.workdir or tempfile.mkdtemp(prefix="runner-"))
    q = host.subscribe(None)
    spec = {"provider": "deepseek", "model": args.model} if args.model else None
    t = await host.start(project=args.project, title=args.request[:60], request=args.request, model=spec)

    async def pump():
        while True:
            _print(await q.get())
    p = asyncio.create_task(pump())
    try:
        await host.wait_idle(t.id, timeout=args.timeout)
        # child threads spawned along the way get to finish too
        for tid in list(host.threads):
            await host.wait_idle(tid, timeout=args.timeout)
    finally:
        await asyncio.sleep(0.1)
        p.cancel()
        for tid in list(host.threads):
            await host.stop(tid)


def main():
    ap = argparse.ArgumentParser(prog="runner")
    ap.add_argument("request")
    ap.add_argument("--repo", help="git repo to make worktrees from")
    ap.add_argument("--workdir")
    ap.add_argument("--project", default="local")
    ap.add_argument("--model", help="DeepSeek model name (default $DEEPSEEK_MODEL or deepseek-v4-pro)")
    ap.add_argument("--timeout", type=float, default=1800)
    asyncio.run(amain(ap.parse_args()))


if __name__ == "__main__":
    main()
