"""
Cursor-based file tracking for the file-drop sensor.

Uses Dagster's ``context.cursor`` (a single JSON string persisted between
sensor ticks) to remember which files have been processed and their
fingerprints.  A file whose fingerprint has changed since the last tick
is treated as *new* (re-processed).

Supports three tracking strategies:
- ``mtime`` (default): Uses mtime + size — fast, no I/O overhead.
- ``checksum``: Uses MD5 content hash — catches in-place modifications
  even when mtime is not updated (e.g., on some network mounts).
- ``custom``: Uses a user-provided function via the expression registry.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass, field
from fnmatch import fnmatch
from pathlib import Path
from typing import Any, Callable


# ------------------------------------------------------------------
# Fingerprint
# ------------------------------------------------------------------


@dataclass(frozen=True)
class FileFingerprint:
    """Lightweight identity of a file."""

    path: str
    mtime: float = 0.0
    size: int = 0
    checksum: str = ""
    etag: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FileFingerprint:
        return cls(
            path=data["path"],
            mtime=data.get("mtime", 0.0),
            size=data.get("size", 0),
            checksum=data.get("checksum", ""),
            etag=data.get("etag", ""),
        )


# ------------------------------------------------------------------
# File tracker
# ------------------------------------------------------------------

CHUNK_SIZE = 65_536


class FileTracker:
    """Stateless helpers that operate on a ``dict[str, FileFingerprint]``
    state object serialised to / from the Dagster cursor string."""

    # ---- Cursor serialisation ----

    @staticmethod
    def deserialize_cursor(cursor: str | None) -> dict[str, FileFingerprint]:
        if not cursor:
            return {}
        try:
            raw: dict[str, dict[str, Any]] = json.loads(cursor)
            return {k: FileFingerprint.from_dict(v) for k, v in raw.items()}
        except (json.JSONDecodeError, KeyError, TypeError):
            return {}

    @staticmethod
    def serialize_cursor(state: dict[str, FileFingerprint]) -> str:
        return json.dumps({k: v.to_dict() for k, v in state.items()})

    # ---- Fingerprinting ----

    @staticmethod
    def fingerprint_file(
        path: Path,
        strategy: str = "mtime",
    ) -> FileFingerprint:
        stat = path.stat()
        checksum = ""

        if strategy == "checksum":
            h = hashlib.md5()
            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(CHUNK_SIZE), b""):
                    h.update(chunk)
            checksum = h.hexdigest()

        return FileFingerprint(
            path=str(path.resolve()),
            mtime=stat.st_mtime,
            size=stat.st_size,
            checksum=checksum,
        )

    # ---- Change detection ----

    @staticmethod
    def _is_changed(
        prev: FileFingerprint,
        curr: FileFingerprint,
        strategy: str,
    ) -> bool:
        if strategy == "checksum":
            return prev.checksum != curr.checksum
        # mtime (default)
        return prev.mtime != curr.mtime or prev.size != curr.size

    # ---- Detection ----

    @staticmethod
    def detect_new_or_modified(
        directory: Path,
        pattern: str,
        known: dict[str, FileFingerprint],
        strategy: str = "mtime",
        filter_fn: Callable[[Path], bool] | None = None,
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
        strategy:
            ``"mtime"`` (default) or ``"checksum"`` (MD5 content hash).
        filter_fn:
            Optional user-provided filter.  Called with each ``Path``;
            return ``True`` to include the file, ``False`` to skip.

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
            if filter_fn and not filter_fn(entry):
                continue

            fp = FileTracker.fingerprint_file(entry, strategy=strategy)
            abs_path = str(entry.resolve())

            prev = known.get(abs_path)
            if prev is None or FileTracker._is_changed(prev, fp, strategy):
                new_files.append(entry.resolve())
                updated_state[abs_path] = fp

        return new_files, updated_state
