"""
File-drop sensor handler.

Watches a local directory or S3 bucket for new or modified files, resolves
dynamic path/pattern expressions on each tick, tracks processing state via
the Dagster cursor, and yields ``RunRequest`` s with the matched file list.

Supports:
- Local filesystem (default)
- S3 (``source: s3``) with AWS profile or credential-based auth
- Tracking strategies: ``mtime`` (default), ``checksum`` (MD5 hash)
- User-defined filter functions via expression registry
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import dagster as dg

from framework.core.sensors.sensor_registry import sensor_handler
from framework.model.config_models import SensorConfig, SensorType
from framework.utils.expr_eval import evaluate_expr, get_expr_registry
from framework.utils.file_tracker import FileTracker


@sensor_handler(SensorType.file_drop)
def handle_file_drop_sensor(config: SensorConfig) -> Callable:
    """
    Build a Dagster sensor that detects new / modified files in a
    local directory or S3 bucket and emits one RunRequest per file.
    """
    sensor_cfg = config.config or {}
    source: str = sensor_cfg.get("source", "local")
    file_path_template: str = sensor_cfg.get("file_path", "")
    file_pattern_template: str = sensor_cfg.get("file_pattern", "*")
    job_name: str | None = config.trigger.target
    partition_name: str | None = config.partition_name

    # Tracking strategy
    tracking_cfg: dict = sensor_cfg.get("tracking", {})
    tracking_strategy: str = tracking_cfg.get("strategy", "mtime")

    # User-defined filter function name (from expr registry)
    filter_fn_name: str | None = tracking_cfg.get("filter_fn")

    # S3-specific config
    s3_bucket: str = sensor_cfg.get("bucket", "")
    s3_prefix_template: str = sensor_cfg.get("prefix", "")
    s3_resource_name: str = sensor_cfg.get("resource", "")

    # ------------------------------------------------------------------
    # Fail-fast validation (definition time)
    # ------------------------------------------------------------------
    if source == "local" and not file_path_template:
        raise ValueError(f"Sensor '{config.name}': config.file_path is required")

    if source == "s3" and not s3_bucket:
        raise ValueError(
            f"Sensor '{config.name}': config.bucket is required for S3 source"
        )

    if not job_name:
        raise ValueError(
            f"Sensor '{config.name}': trigger.target (job name) is required"
        )

    if partition_name is None:
        raise ValueError(
            f"Sensor '{config.name}': partition_name is required for file-drop sensors"
        )

    # Resolve the user filter function at definition time (if provided)
    user_filter_fn = None
    if filter_fn_name:
        registry = get_expr_registry()
        if filter_fn_name not in registry:
            raise ValueError(
                f"Sensor '{config.name}': filter function '{filter_fn_name}' "
                f"not found in expression registry. Register it with "
                f"@expr_function or load via user_modules."
            )
        user_filter_fn = registry[filter_fn_name]

    # ------------------------------------------------------------------
    # Build the right sensor based on source type
    # ------------------------------------------------------------------
    if source == "s3":
        required_keys = {s3_resource_name} if s3_resource_name else set()

        @dg.sensor(
            name=config.name,
            description=config.description,
            job_name=job_name,
            minimum_interval_seconds=config.trigger.minimum_interval_seconds or 30,
            default_status=dg.DefaultSensorStatus.RUNNING,
            required_resource_keys=required_keys,
        )
        def s3_file_drop_sensor(context: dg.SensorEvaluationContext):
            from framework.utils.s3_file_tracker import S3FileTracker

            resolved_prefix = evaluate_expr(s3_prefix_template)
            resolved_pattern = evaluate_expr(file_pattern_template)

            context.log.info(
                f"Scanning s3://{s3_bucket}/{resolved_prefix} "
                f"pattern='{resolved_pattern}'"
            )

            s3_res = getattr(context.resources, s3_resource_name)
            s3_client = s3_res["client"]

            known = S3FileTracker.deserialize_cursor(context.cursor)

            new_keys, updated_state = S3FileTracker.detect_new_or_modified(
                s3_client=s3_client,
                bucket=s3_bucket,
                prefix=resolved_prefix,
                pattern=resolved_pattern,
                known=known,
                filter_fn=user_filter_fn,
            )

            if not new_keys:
                context.log.info("No new or modified S3 objects detected.")
                return dg.SkipReason("No new or modified S3 objects")

            context.log.info(
                f"Detected {len(new_keys)} new/modified S3 object(s)"
            )

            context.instance.add_dynamic_partitions(partition_name, new_keys)
            context.update_cursor(S3FileTracker.serialize_cursor(updated_state))

            for s3_key in new_keys:
                yield dg.RunRequest(
                    run_key=s3_key,
                    partition_key=s3_key,
                    tags={
                        "sensor": config.name,
                        "file_path": s3_key,
                        "source": "s3",
                        "bucket": s3_bucket,
                        "pattern": resolved_pattern,
                    },
                )

        return s3_file_drop_sensor

    # ------------------------------------------------------------------
    # Local filesystem sensor
    # ------------------------------------------------------------------
    @dg.sensor(
        name=config.name,
        description=config.description,
        job_name=job_name,
        minimum_interval_seconds=config.trigger.minimum_interval_seconds or 30,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def file_drop_sensor(context: dg.SensorEvaluationContext):

        resolved_path = evaluate_expr(file_path_template)
        resolved_pattern = evaluate_expr(file_pattern_template)
        directory = Path(resolved_path)

        context.log.info(
            f"Scanning directory='{directory}' pattern='{resolved_pattern}' "
            f"strategy='{tracking_strategy}'"
        )

        if not directory.is_dir():
            context.log.warning(f"Directory does not exist: {directory}")
            return dg.SkipReason(f"Directory not found: {directory}")

        known = FileTracker.deserialize_cursor(context.cursor)

        new_files, updated_state = FileTracker.detect_new_or_modified(
            directory=directory,
            pattern=resolved_pattern,
            known=known,
            strategy=tracking_strategy,
            filter_fn=user_filter_fn,
        )

        if not new_files:
            context.log.info("No new or modified files detected.")
            return dg.SkipReason("No new or modified files")

        matched_paths = sorted(str(p) for p in new_files)

        context.log.info(
            f"Detected {len(matched_paths)} new/modified file(s): {matched_paths}"
        )

        context.instance.add_dynamic_partitions(
            partition_name,
            matched_paths,
        )

        context.update_cursor(FileTracker.serialize_cursor(updated_state))

        for file_path in matched_paths:
            yield dg.RunRequest(
                run_key=file_path,
                partition_key=file_path,
                tags={
                    "sensor": config.name,
                    "file_path": file_path,
                    "directory": str(directory),
                    "pattern": resolved_pattern,
                    "tracking": tracking_strategy,
                },
            )

    return file_drop_sensor
