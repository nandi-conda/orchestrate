from orchestrate_desktop.model import (K_ASSISTANT, K_STATE, K_THINKING, K_TOOL_CALL, K_TOOL_RESULT, K_USER, counts,
                                       dialog_items, join_path, size_text)


def ev(seq, kind, tid="t_a", **kw):
    return {"seq": seq, "kind": kind, "thread_id": tid, "label": "x", "text": f"e{seq}", **kw}


def test_reasoning_runs_group_across_hidden_rows_only():
    events = [ev(1, K_USER), ev(2, K_THINKING), ev(3, K_TOOL_CALL), ev(4, K_TOOL_RESULT), ev(5, K_THINKING),
              ev(6, K_STATE), ev(7, K_THINKING, "t_b"), ev(8, K_ASSISTANT), ev(9, K_THINKING)]
    items = dialog_items(events, activity=False)
    assert [(w, [e["seq"] for e in es]) for w, es in items] == [
        ("event", [1]), ("thinking", [2, 5]), ("thinking", [7]), ("event", [8]), ("thinking", [9])]


def test_streaming_reasoning_stands_alone_and_project_activity_leaves_it_out():
    events = [ev(1, K_THINKING), ev(2, K_THINKING, live=1), ev(3, K_ASSISTANT, live=1)]
    assert [(w, [e["seq"] for e in es]) for w, es in dialog_items(events, False)] == [
        ("thinking", [1]), ("event", [2]), ("event", [3])]
    assert dialog_items(events, True) == []


def test_counts_and_helpers():
    events = [ev(1, K_USER), ev(2, K_TOOL_CALL), ev(3, K_TOOL_RESULT), ev(4, K_STATE)]
    assert counts(events, False) == (1, 2)
    assert size_text(3) == "3 B" and size_text(4096) == "4 KB" and size_text(3 << 20) == "3 MB"
    assert join_path("", "a") == "a" and join_path("a", "b") == "a/b"
