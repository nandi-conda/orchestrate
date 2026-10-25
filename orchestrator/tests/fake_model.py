"""A fake OpenAI-compatible chat endpoint, so the real runner and the model
router can be exercised without calling DeepSeek.

Point DEEPSEEK_BASE_URL at it. Router prompts get a one-thread plan; agent
turns get one `bash` tool call, then a final answer once the tool result is in.
Streaming requests (the runner's) get the reply as SSE chunks, with reasoning
first, spread over a few seconds so the UI's live view can be watched.
"""

import asyncio
import json
import sys

import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Route


def reply(message: dict) -> JSONResponse:
    return JSONResponse({"choices": [{"message": message, "finish_reason": "stop"}],
                         "usage": {"prompt_tokens": 1, "completion_tokens": 1}})


def stream(message: dict) -> StreamingResponse:
    def chunk(delta: dict) -> str:
        return f"data: {json.dumps({'choices': [{'index': 0, 'delta': delta}]})}\n\n"

    async def gen():
        yield ": keep-alive\n\n"
        for word in "Let me look at what is asked and pick the next step.".split():
            yield chunk({"reasoning_content": word + " "})
            await asyncio.sleep(0.15)
        content = message.get("content") or ""
        for i in range(0, len(content), 6):
            yield chunk({"content": content[i:i + 6]})
            await asyncio.sleep(0.1)
        for i, tc in enumerate(message.get("tool_calls") or []):
            yield chunk({"tool_calls": [{"index": i, "id": tc["id"], "type": "function",
                                         "function": {"name": tc["function"]["name"], "arguments": ""}}]})
            args = tc["function"]["arguments"]
            for j in range(0, len(args), 5):
                yield chunk({"tool_calls": [{"index": i, "function": {"arguments": args[j:j + 5]}}]})
                await asyncio.sleep(0.1)
        yield f"data: {json.dumps({'choices': [], 'usage': {'prompt_tokens': 1, 'completion_tokens': 1}})}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")


async def chat(req: Request):
    body = await req.json()
    if body.get("stream"):
        return stream(agent_reply(body["messages"]))
    msgs = body["messages"]
    if msgs[0]["role"] == "system" and msgs[0]["content"].startswith("You route requests"):
        ask = json.loads(msgs[-1]["content"])
        plan = {"spawn": [{"title": "Fake routed thread", "request": ask["request"]}], "send": [],
                "why": "fake model plan"}
        return reply({"role": "assistant", "content": json.dumps(plan)})
    return reply(agent_reply(msgs))


def agent_reply(msgs: list[dict]) -> dict:
    if msgs[-1]["role"] == "tool":
        return {"role": "assistant", "content": "Done: ran the command."}
    return {"role": "assistant", "content": "Running it now.",
            "tool_calls": [{"id": "call_1", "type": "function",
                            "function": {"name": "bash", "arguments": json.dumps({"command": "echo hi"})}}]}


app = Starlette(routes=[Route("/chat/completions", chat, methods=["POST"])])

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=int(sys.argv[1]) if len(sys.argv) > 1 else 9999, log_level="warning")
