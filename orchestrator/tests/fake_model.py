"""A fake OpenAI-compatible chat endpoint, so the real runner and the model
router can be exercised without calling DeepSeek.

Point DEEPSEEK_BASE_URL at it. Router prompts get a one-thread plan; agent
turns get one `bash` tool call, then a final answer once the tool result is in.
"""

import json
import sys

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route


def reply(message: dict) -> JSONResponse:
    return JSONResponse({"choices": [{"message": message, "finish_reason": "stop"}],
                         "usage": {"prompt_tokens": 1, "completion_tokens": 1}})


async def chat(req: Request):
    body = await req.json()
    msgs = body["messages"]
    if msgs[0]["role"] == "system" and msgs[0]["content"].startswith("You route requests"):
        ask = json.loads(msgs[-1]["content"])
        plan = {"spawn": [{"title": "Fake routed thread", "request": ask["request"]}], "send": [],
                "why": "fake model plan"}
        return reply({"role": "assistant", "content": json.dumps(plan)})
    if msgs[-1]["role"] == "tool":
        return reply({"role": "assistant", "content": "Done: ran the command."})
    return reply({"role": "assistant", "content": "",
                  "tool_calls": [{"id": "call_1", "type": "function",
                                  "function": {"name": "bash", "arguments": json.dumps({"command": "echo hi"})}}]})


app = Starlette(routes=[Route("/chat/completions", chat, methods=["POST"])])

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(sys.argv[1]) if len(sys.argv) > 1 else 9999, log_level="warning")
