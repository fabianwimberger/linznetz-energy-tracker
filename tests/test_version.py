"""Guards against version drift between version.py and pyproject.toml."""

import tomllib
from pathlib import Path

from version import __version__

_REPO_ROOT = Path(__file__).resolve().parents[1]


def test_version_matches_pyproject() -> None:
    with (_REPO_ROOT / "pyproject.toml").open("rb") as fh:
        pyproject = tomllib.load(fh)
    assert __version__ == pyproject["project"]["version"]
