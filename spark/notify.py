"""macOS 系统通知。"""

from __future__ import annotations

import subprocess


def _escape(text):
    return str(text).replace("\\", "\\\\").replace('"', '\\"')


def notify(title, body):
    script = (
        f'display notification "{_escape(body)}" '
        f'with title "{_escape(title)}" sound name "Basso"'
    )
    try:
        subprocess.run(
            ["osascript", "-e", script],
            check=False,
            timeout=10,
            capture_output=True,
        )
        return True
    except Exception:
        return False
