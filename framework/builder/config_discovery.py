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

from pathlib import Path
from typing import Any

import yaml


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
    """Parse a YAML file and return its contents as a dict."""
    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping in {path}, got {type(data).__name__}")
    return data


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

