"""Build the ultra UI into ui/dist: app.py compiled to app.wasm (with ultra's
ultra.js and ui.css beside it), and the page's own files copied in. The
`build-ui` pixi task and the conda recipe (recipe/recipe.yaml) both run this."""

import shutil
import subprocess
from pathlib import Path

UI = Path(__file__).resolve().parent
DIST = UI / "dist"
PAGE = ("index.html", "imports.js", "markdown.js", "app.css", "icon.svg")


def main() -> None:
    DIST.mkdir(exist_ok=True)
    subprocess.run(["ultra", "build", str(UI / "app.py"), "-o", str(DIST / "app.wasm")], check=True)
    for name in PAGE:
        shutil.copy(UI / name, DIST / name)


main()
