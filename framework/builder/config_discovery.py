"""
Config file discovery and merging.

Discovers ``.macro`` and ``.resource`` files from a config directory
and merges them into a single unified config dict that can be fed
into the ref-resolver and then into Pydantic validation.

File extensions (all YAML under the hood):
    *.resource  — must contain a ``resources`` list
    *.macro     — may contain ``assets``, ``jobs``, ``sensors`` lists
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

_ENV_RE: re.Pattern[str] = re.compile(
    r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}"
)


def _resolve_env(value: str) -> str:
    """Resolve ``${VAR:-default}`` and ``${VAR}`` patterns using os.environ."""

    def _replace(m: re.Match[str]) -> str:
        var_name = m.group(1)
        default = m.group(2)
        env_val = os.environ.get(var_name)
        if env_val is not None:
            return env_val
        if default is not None:
            return default
        return m.group(0)

    return _ENV_RE.sub(_replace, value)


def _resolve_config(obj: Any) -> Any:
    """Walk a config structure and resolve env vars in all string values."""
    if isinstance(obj, str):
        return _resolve_env(obj)
    if isinstance(obj, dict):
        return {k: _resolve_config(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_resolve_config(item) for item in obj]
    return obj


# ------------------------------------------------------------------
# Discovery
# ------------------------------------------------------------------


def discover_files(config_dir: Path) -> tuple[list[Path], list[Path]]:
    """Glob ``.resource`` and ``.macro`` files from *config_dir*.

    Returns
    -------
    Tuple of (resource_paths, macro_paths), each sorted by filename
    for deterministic load order.
    """
    if not config_dir.is_dir():
        raise FileNotFoundError(f"Config directory not found: {config_dir}")

    resource_paths = sorted(config_dir.glob("*.resource"))
    macro_paths = sorted(config_dir.glob("*.macro"))
    return resource_paths, macro_paths


# ------------------------------------------------------------------
# Merging
# ------------------------------------------------------------------

_MERGEABLE_SECTIONS = ("file_formatters", "assets", "jobs", "sensors")


def _load_yaml(path: Path) -> dict[str, Any]:
    """Parse a YAML file, resolve env vars, and return its contents."""
    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping in {path}, got {type(data).__name__}")
    return _resolve_config(data)


def _check_duplicates(
    existing_names: dict[str, Path],
    items: list[dict[str, Any]],
    section: str,
    source_file: Path,
) -> None:
    """Raise on duplicate ``name`` values within a section."""
    for item in items:
        name = item.get("name")
        if not name:
            continue
        if name in existing_names:
            raise ValueError(
                f"Duplicate {section} name '{name}' found in "
                f"{source_file} — already defined in {existing_names[name]}"
            )
        existing_names[name] = source_file


def load_and_merge(
    resource_paths: list[Path],
    macro_paths: list[Path],
) -> dict[str, Any]:
    """Load all files and merge into a single config dict.

    Returns
    -------
    ``{"resources": [...], "assets": [...], "jobs": [...], "sensors": [...]}``

    Raises
    ------
    ValueError
        On duplicate ``name`` within any section.
    """
    merged: dict[str, Any] = {
        "resources": [],
        "file_formatters": [],
        "assets": [],
        "jobs": [],
        "sensors": [],
    }

    # Track names per section for duplicate detection
    seen: dict[str, dict[str, Path]] = {
        "resources": {},
        "file_formatters": {},
        "assets": {},
        "jobs": {},
        "sensors": {},
    }

    # ---- .resource files ----
    for path in resource_paths:
        data = _load_yaml(path)
        items = data.get("resources", [])
        if not isinstance(items, list):
            raise ValueError(f"'resources' must be a list in {path}")
        _check_duplicates(seen["resources"], items, "resource", path)
        merged["resources"].extend(items)

    # ---- .macro files ----
    for path in macro_paths:
        data = _load_yaml(path)
        for section in _MERGEABLE_SECTIONS:
            items = data.get(section, [])
            if not isinstance(items, list):
                raise ValueError(f"'{section}' must be a list in {path}")
            _check_duplicates(seen[section], items, section, path)
            merged[section].extend(items)

    return merged

