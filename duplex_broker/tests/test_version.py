from __future__ import annotations

import re
from pathlib import Path

from duplex_broker import __version__

ROOT = Path(__file__).resolve().parents[1]


def test_every_version_names_the_same_release() -> None:
    # /health, the User-Agent and the MCP server info read __version__; Supervisor reads config.yaml.
    pyproject = re.search(r'^version = "([^"]+)"', (ROOT / "pyproject.toml").read_text(), re.M)
    config = re.search(r'^version: "([^"]+)"', (ROOT / "config.yaml").read_text(), re.M)
    changelog = re.search(r"^## (\S+)", (ROOT / "CHANGELOG.md").read_text(), re.M)
    assert pyproject and config and changelog
    assert {pyproject[1], config[1], changelog[1]} == {__version__}
