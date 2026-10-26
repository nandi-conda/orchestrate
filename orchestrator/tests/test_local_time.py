import os
import time

import pytest

from control.service import local_line, local_log, when


@pytest.fixture
def berlin():
    old = os.environ.get("TZ")
    os.environ["TZ"] = "Europe/Berlin"
    time.tzset()
    yield
    if old is None:
        del os.environ["TZ"]
    else:
        os.environ["TZ"] = old
    time.tzset()


def test_runner_line_in_local_time(berlin):
    # 2026-10-26 is after the switch from CEST, so Berlin is UTC+1.
    assert local_line("2026-10-26T04:53:21.042Z INFO: runner.harness: up") == "2026-10-26 05:53:21.042 INFO: runner.harness: up"
    assert local_line("2026-07-01T23:30:00.000Z INFO: x") == "2026-07-02 01:30:00.000 INFO: x"


def test_lines_without_a_runner_time_stand(berlin):
    for line in ("INFO:     Started server process", "[sprite] runner service exited with code 1", ""):
        assert local_line(line) == line


def test_log_text(berlin):
    assert local_log("2026-10-26T04:53:21.042Z a\nplain\n") == "2026-10-26 05:53:21.042 a\nplain\n"


def test_when_is_local(berlin):
    assert when(1792990401) == "2026-10-26 05:53:21 CET"
