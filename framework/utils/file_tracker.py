"""
Cursor-based file tracking for the file-drop sensor.

Uses Dagster's ``context.cursor`` (a single JSON string persisted between
sensor ticks) to remember which files have been processed and their
fingerprints (mtime + size).  A file whose fingerprint has changed since the
last tick is treated as *new* (re-processed).
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from fnmatch import fnmatch
from pathlib import Path
from typing import Any


# ------------------------------------------------------------------
# Fingerprint
# ------------------------------------------------------------------


@dataclass(frozen=True)
class FileFingerprint:
    """Lightweight identity of a file on disk."""

    path: str
    mtime: float
    size: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FileFingerprint:
        return cls(path=data["path"], mtime=data["mtime"], size=data["size"])


# ------------------------------------------------------------------
# File tracker
# ------------------------------------------------------------------


class FileTracker:
    """Stateless helpers that operate on a ``dict[str, FileFingerprint]``
    state object serialised to / from the Dagster cursor string."""

    # ---- Cursor serialisation ----

    @staticmethod
    def deserialize_cursor(cursor: str | None) -> dict[str, FileFingerprint]:
        """Decode the Dagster cursor string into a path → fingerprint map."""
        if not cursor:
            return {}
        try:
            raw: dict[str, dict[str, Any]] = json.loads(cursor)
            return {k: FileFingerprint.from_dict(v) for k, v in raw.items()}
        except (json.JSONDecodeError, KeyError, TypeError):
            return {}

    @staticmethod
    def serialize_cursor(state: dict[str, FileFingerprint]) -> str:
        """Encode the state map back to a JSON cursor string."""
        return json.dumps({k: v.to_dict() for k, v in state.items()})

    # ---- Fingerprinting ----

    @staticmethod
    def fingerprint_file(path: Path) -> FileFingerprint:
        """Return the fingerprint of a single file."""
        stat = path.stat()
        return FileFingerprint(
            path=str(path.resolve()),
            mtime=stat.st_mtime,
            size=stat.st_size,
        )

    # ---- Detection ----

    @staticmethod
    def detect_new_or_modified(
        directory: Path,
        pattern: str,
        known: dict[str, FileFingerprint],
    ) -> tuple[list[Path], dict[str, FileFingerprint]]:
        """Scan *directory* for files matching *pattern* and return new/changed ones.

        Parameters
        ----------
        directory:
            Resolved directory path to scan.
        pattern:
            Glob-style pattern (evaluated via ``fnmatch``).
        known:
            Previously tracked fingerprints (from cursor).

        Returns
        -------
        A tuple of:
        - list of absolute ``Path`` objects for new or modified files.
        - updated state dict (to persist back into the cursor).
        """
        if not directory.is_dir():
            return [], dict(known)

        new_files: list[Path] = []
        updated_state: dict[str, FileFingerprint] = dict(known)

        for entry in sorted(directory.iterdir()):
            if not entry.is_file():
                continue
            if not fnmatch(entry.name, pattern):
                continue

            fp = FileTracker.fingerprint_file(entry)
            abs_path = str(entry.resolve())

            prev = known.get(abs_path)
            if prev is None or prev.mtime != fp.mtime or prev.size != fp.size:
                new_files.append(entry.resolve())
                updated_state[abs_path] = fp

        return new_files, updated_state


