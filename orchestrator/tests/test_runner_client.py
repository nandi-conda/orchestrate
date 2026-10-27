"""RunnerClient: a runner's refusal carries its own words."""

import httpx
import pytest

from control.runner_client import RunnerClient, RunnerError
from control.steps import describe

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


def client(status: int, **answer) -> RunnerClient:
    c = RunnerClient("https://worker.invalid")
    c.http = httpx.AsyncClient(base_url=c.url, transport=httpx.MockTransport(
        lambda req: httpx.Response(status, request=req, **answer)))
    return c


async def test_a_refused_bundle_says_why():
    c = client(400, json={"error": "refs the orchestrator may not set: refs/heads/x"})
    with pytest.raises(RunnerError) as e:
        await c.push_repo("p_1", b"bundle", "main", {"refs/heads/x": "0" * 40})
    assert e.value.status == 400
    assert e.value.error == "refs the orchestrator may not set: refs/heads/x"
    assert describe(e.value) == ("RunnerError: the worker answered 400 to POST /projects/p_1/repo: "
                                 "refs the orchestrator may not set: refs/heads/x")


async def test_an_answer_that_is_not_json_is_given_as_it_is():
    c = client(502, text="bad gateway from the proxy\n")
    with pytest.raises(RunnerError, match="answered 502 to GET /health: bad gateway from the proxy$"):
        await c.health()


async def test_an_empty_answer_names_the_status():
    c = client(404)
    with pytest.raises(RunnerError, match="answered 404 to POST /threads/t_1/stop: 404 Not Found$"):
        await c.stop("t_1")


async def test_a_proxys_error_page_is_given_as_its_title():
    page = "<!DOCTYPE html><html><head><title>502 Bad Gateway · Sprites</title></head><body>" + "x" * 5000 + "</body></html>"
    c = client(502, text=page, headers={"content-type": "text/html; charset=utf-8"})
    with pytest.raises(RunnerError, match=r"answered 502 to GET /health: 502 Bad Gateway · Sprites$"):
        await c.health()
