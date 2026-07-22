"""
S3-aware file tracking for the file-drop sensor.

Lists objects in an S3 bucket/prefix and tracks state using the S3
ETag (content hash), LastModified, and Size.  This avoids reprocessing
files that have already been ingested.

The state dict is serialised to the same Dagster cursor format as
the local ``FileTracker``.
"""

from __future__ import annotations

from fnmatch import fnmatch
from typing import Any, Callable

from framework.utils.file_tracker import FileFingerprint, FileTracker


class S3FileTracker:
    """Stateless helpers for tracking S3 objects between sensor ticks."""

    @staticmethod
    def detect_new_or_modified(
        s3_client: Any,
        bucket: str,
        prefix: str,
        pattern: str,
        known: dict[str, FileFingerprint],
        filter_fn: Callable[[str], bool] | None = None,
    ) -> tuple[list[str], dict[str, FileFingerprint]]:
        """List objects under *prefix* and return new or changed keys.

        Parameters
        ----------
        s3_client:
            A ``boto3`` S3 client.
        bucket:
            S3 bucket name.
        prefix:
            Key prefix (folder path in S3). Trailing ``/`` is added if missing.
        pattern:
            Glob-style filename pattern (matched against the filename
            portion of the key, not the full key).
        known:
            Previously tracked fingerprints (from cursor).
        filter_fn:
            Optional user-provided filter.  Called with the full S3 key;
            return ``True`` to include, ``False`` to skip.

        Returns
        -------
        A tuple of:
        - list of full S3 keys (``s3://bucket/key``) for new or modified objects.
        - updated state dict (to persist back into the cursor).
        """
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        new_keys: list[str] = []
        updated_state: dict[str, FileFingerprint] = dict(known)

        paginator = s3_client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            for obj in page.get("Contents", []):
                key: str = obj["Key"]

                if key.endswith("/"):
                    continue

                filename = key.rsplit("/", 1)[-1]
                if not fnmatch(filename, pattern):
                    continue

                if filter_fn and not filter_fn(key):
                    continue

                s3_uri = f"s3://{bucket}/{key}"
                etag = obj.get("ETag", "").strip('"')
                last_modified = obj.get("LastModified")
                mtime = last_modified.timestamp() if last_modified else 0.0
                size = obj.get("Size", 0)

                fp = FileFingerprint(
                    path=s3_uri,
                    mtime=mtime,
                    size=size,
                    etag=etag,
                )

                prev = known.get(s3_uri)
                if prev is None or prev.etag != fp.etag:
                    new_keys.append(s3_uri)
                    updated_state[s3_uri] = fp

        return new_keys, updated_state

    deserialize_cursor = FileTracker.deserialize_cursor
    serialize_cursor = FileTracker.serialize_cursor
