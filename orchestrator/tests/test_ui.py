# The UI's own code, checked without a browser: its message numbers (dispatched
# in ui/app.py's update()) and the imports it adds in ui/imports.js, run under
# node against the cases in ui_rows.mjs.
import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
APP = ROOT / "ui" / "app.py"
CASES = Path(__file__).with_name("ui_rows.mjs")


def test_each_message_number_is_its_own():
    """update() tells messages apart by the number alone: two of them sharing
    one would run the other's handler, and nothing would say so."""
    block = APP.read_text().split("# ---- Msg", 1)[1].split("# pane tabs", 1)[0]
    named = re.findall(r"^(\w+): Const\[i32\] = (\d+)", block, re.MULTILINE)
    numbers = [n for _, n in named]
    duplicates = {n for n in numbers if numbers.count(n) > 1}
    assert not duplicates, f"messages sharing a number: {sorted(duplicates)}"


def results():
    out = subprocess.run([shutil.which("node"), str(CASES)], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


@pytest.mark.parametrize("case", results(), ids=lambda c: c["name"])
def test_ui_rows(case):
    assert case["got"] == case["want"]
