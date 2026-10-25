# The UI's markdown renderer (ui/markdown.js), run under node: each case in
# markdown_cases.mjs is a reply and the markup the window shows for it.
import json
import shutil
import subprocess
from pathlib import Path

import pytest

CASES = Path(__file__).with_name("markdown_cases.mjs")


def results():
    out = subprocess.run([shutil.which("node"), str(CASES)], check=True, capture_output=True, text=True).stdout
    return json.loads(out)


@pytest.mark.parametrize("case", results(), ids=lambda c: c["name"])
def test_markdown(case):
    assert case["got"] == case["want"]
