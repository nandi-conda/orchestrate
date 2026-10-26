"""Run one request locally: `pixi run run "add a README" --repo .`"""

from __future__ import annotations

import argparse
import asyncio
import json
import tempfile

from . import sandbox
from .harness import PrimeAgent
from .repo import Repo
from .secrets import get_secret, require
from .thread import ThreadHost
from .web import deepseek_web


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
    workdir = args.workdir or tempfile.mkdtemp(prefix="runner-")
    repo = None
    if args.repo:
        repo = Repo(f"{workdir}/.repo")
        await repo.seed(args.repo)
    host = ThreadHost(agent=PrimeAgent(env={"DEEPSEEK_API_KEY": get_secret("DEEPSEEK_API_KEY")}),
                      web=deepseek_web(), repo=repo, workdir=workdir)
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
        if repo:
            print(f"work landed on origin/{repo.base} in {repo.path}")


def main():
    ap = argparse.ArgumentParser(prog="runner")
    ap.add_argument("request")
    ap.add_argument("--repo", help="git URL or repo path whose branches the threads start from; "
                    "they land on its default branch in the workdir's repository, never on the source")
    ap.add_argument("--workdir")
    ap.add_argument("--project", default="local")
    ap.add_argument("--model", help="DeepSeek model name (default $DEEPSEEK_MODEL or deepseek-flash)")
    ap.add_argument("--timeout", type=float, default=1800)
    args = ap.parse_args()
    require(["DEEPSEEK_API_KEY"])
    sandbox.check()
    PrimeAgent().check()
    asyncio.run(amain(args))


if __name__ == "__main__":
    main()
