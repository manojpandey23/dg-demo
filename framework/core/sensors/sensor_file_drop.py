"""
File-drop sensor handler.

Watches a configured directory for new or modified files, resolves dynamic
path/pattern expressions on each tick, tracks processing state via the
Dagster cursor, and yields ``RunRequest`` s with the matched file list.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable

import dagster as dg

from framework.core.sensors.sensor_registry import sensor_handler
from framework.model.config_models import SensorConfig, SensorType
from framework.utils.expr_eval import evaluate_expr
from framework.utils.file_tracker import FileTracker


@sensor_handler(SensorType.file_drop)
def handle_file_drop_sensor(config: SensorConfig) -> Callable:
    """
    Build a Dagster sensor that detects new / modified files in a directory
    and emits one RunRequest per file (partitioned).
    """
    sensor_cfg = config.config or {}
    file_path_template: str = sensor_cfg.get("file_path", "")
    file_pattern_template: str = sensor_cfg.get("file_pattern", "*")
    job_name: str | None = config.trigger.target
    partition_name: str | None = config.partition_name

    # ------------------------------------------------------------------
    # Fail-fast validation (definition time)
    # ------------------------------------------------------------------
    if not file_path_template:
        raise ValueError(f"Sensor '{config.name}': config.file_path is required")

    if not job_name:
        raise ValueError(
            f"Sensor '{config.name}': trigger.target (job name) is required"
        )

    if partition_name is None:
        raise ValueError(
            f"Sensor '{config.name}': partition_name is required for file-drop sensors"
        )

    # ------------------------------------------------------------------
    # Sensor closure
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
            f"Scanning directory='{directory}' pattern='{resolved_pattern}'"
        )

        if not directory.is_dir():
            context.log.warning(f"Directory does not exist: {directory}")
            return dg.SkipReason(f"Directory not found: {directory}")

        # --------------------------------------------------------------
        # Load tracked state
        # --------------------------------------------------------------
        known = FileTracker.deserialize_cursor(context.cursor)

        new_files, updated_state = FileTracker.detect_new_or_modified(
            directory=directory,
            pattern=resolved_pattern,
            known=known,
        )

        if not new_files:
            context.log.info("No new or modified files detected.")
            return dg.SkipReason("No new or modified files")

        matched_paths = sorted(str(p) for p in new_files)

        context.log.info(
            f"Detected {len(matched_paths)} new/modified file(s): {matched_paths}"
        )

        # --------------------------------------------------------------
        # Register dynamic partitions (ONCE)
        # --------------------------------------------------------------
        context.instance.add_dynamic_partitions(
            partition_name,
            matched_paths,
        )

        # --------------------------------------------------------------
        # Persist cursor ONCE
        # --------------------------------------------------------------
        context.update_cursor(FileTracker.serialize_cursor(updated_state))

        # --------------------------------------------------------------
        # Emit ONE RunRequest per file
        # --------------------------------------------------------------
        for file_path in matched_paths:
            yield dg.RunRequest(
                run_key=file_path,
                partition_key=file_path,
                tags={
                    "sensor": config.name,
                    "file_path": file_path,
                    "directory": str(directory),
                    "pattern": resolved_pattern,
                },
            )

    return file_drop_sensor
