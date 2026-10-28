"""Draws the menu shortcut's icons from the app icon (orchestrator/ui/icon.svg)
into a Menu directory: orchestrate.png (Linux), orchestrate.icns (macOS) and
orchestrate.ico (Windows), the names recipe/menu/orchestrate.json gives as
{{ MENU_DIR }}/orchestrate.{{ ICON_EXT }}.

    python recipe/menu/icons.py orchestrator/ui/icon.svg $PREFIX/Menu
"""

import io
import sys
from pathlib import Path

import cairosvg
from PIL import Image


def main() -> None:
    svg, menu = Path(sys.argv[1]), Path(sys.argv[2])
    png = cairosvg.svg2png(url=str(svg), output_width=1024, output_height=1024)
    icon = Image.open(io.BytesIO(png)).convert("RGBA")
    icon.resize((512, 512), Image.LANCZOS).save(menu / "orchestrate.png")
    icon.save(menu / "orchestrate.icns")
    icon.save(menu / "orchestrate.ico", sizes=[(s, s) for s in (16, 24, 32, 48, 64, 128, 256)])


if __name__ == "__main__":
    main()
