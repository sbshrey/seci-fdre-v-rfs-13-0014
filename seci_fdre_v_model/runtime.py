"""Runtime helpers for resolving bundled assets and desktop workspace paths."""

from __future__ import annotations

import os
import sys
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def bundled_root() -> Path:
    frozen_root = getattr(sys, "_MEIPASS", None)
    if frozen_root:
        return Path(frozen_root).resolve()
    return repo_root()


def resolve_seed_source_config_path() -> Path:
    return (bundled_root() / "config" / "project.yaml").resolve()


def default_windows_workspace_root() -> Path:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return (Path(local_app_data) / "SECI FDRE V").resolve()
    return (Path.home() / "AppData" / "Local" / "SECI FDRE V").resolve()
