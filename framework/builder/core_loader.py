"""
Framework Loader — YAML-Driven with multi-file discovery.

Supports two modes:

1. **Multi-file discovery mode** (default)::

       loader = FrameworkLoader(config_dir=Path("configs"))
       defs   = loader.get_definitions()

   Discovers all ``.resource`` and ``.macro`` files in *config_dir*,
   merges them, resolves ``ref()`` placeholders via JMESPath, then
   builds Dagster assets / jobs / sensors / checks / resources.

2. **Legacy single-file mode** (backward compatible)::

       loader = FrameworkLoader(config_dir, resources_yaml="resources.yaml")
       defs   = loader.get_definitions("framework_pipeline.yaml")
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Tuple

import dagster as dg
import yaml
from pydantic import ValidationError

from framework.builder.asset_builder import AssetBuilder
from framework.builder.config_discovery import discover_files, load_and_merge
from framework.builder.job_builder import JobBuilder
from framework.builder.resources_builder import ResourceBuilder
from framework.builder.sensor_builder import SensorBuilder
from framework.cdc.cdc_builder import CDCBuilder
from framework.model.config_models import FrameworkPipelineConfig
from framework.validation_check_builder import ValidationCheckBuilder


class FrameworkLoader:
    """Load framework configurations from YAML.

    Parameters
    ----------
    config_dir:
        Path to the directory containing config files.
    resources_yaml:
        *Legacy only* — name of a single resources YAML file.
        When omitted the loader scans for ``.resource`` / ``.macro``
        files automatically.
    environment:
        ``"local"`` or ``"prod"``.  Affects resource overrides.
    """

    def __init__(
        self,
        config_dir: Path,
        resources_yaml: str | None = None,
        environment: str = "local",
    ) -> None:
        self.config_dir = Path(config_dir)
        self.resources_yaml = resources_yaml
        self.environment = environment

        self.resources: Dict[str, dg.ResourceDefinition] = {}
        self._pipeline_config: FrameworkPipelineConfig | None = None

        if self.resources_yaml:
            # Legacy: load a single resources file eagerly
            self._load_resources_from_file()
        else:
            # Default: discover .resource + .macro and build everything
            self._discover_and_load()

    # =================================================================
    # Multi-file discovery (default path)
    # =================================================================

    def _discover_and_load(self) -> None:
        """Scan *config_dir* for ``.resource`` / ``.macro`` files,
        merge, resolve ``ref()`` placeholders, and build all Dagster
        objects in one shot.
        """
        # Step 1 — discover
        resource_paths, macro_paths = discover_files(self.config_dir)

        dg.get_dagster_logger().info(
            f"📂 Discovered {len(resource_paths)} .resource file(s), "
            f"{len(macro_paths)} .macro file(s) in {self.config_dir}"
        )

        if not resource_paths and not macro_paths:
            dg.get_dagster_logger().warning(
                f"⚠️  No .resource or .macro files found in {self.config_dir}"
            )
            self._pipeline_config = FrameworkPipelineConfig()
            return

        # Step 2 — merge
        merged = load_and_merge(resource_paths, macro_paths)

        # Step 3 — resolve ref() placeholders
        # resolved = resolve_refs(merged)

        # Step 4 — build resources
        resource_dicts = merged.pop("resources", [])
        self.resources = self._build_resources(resource_dicts)

        dg.get_dagster_logger().info(
            f"✅ Built {len(self.resources)} resource(s) from "
            f"{len(resource_paths)} .resource file(s)"
        )

        # Step 5 — validate merged pipeline config
        try:
            self._pipeline_config = FrameworkPipelineConfig(**merged)
        except ValidationError as e:
            raise ValueError(f"Merged configuration validation failed: {e}") from e

        dg.get_dagster_logger().info(
            f"✅ Loaded {len(self._pipeline_config.assets)} asset(s), "
            f"{len(self._pipeline_config.jobs)} job(s), "
            f"{len(self._pipeline_config.sensors)} sensor(s) from "
            f"{len(macro_paths)} .macro file(s)"
        )

    # =================================================================
    # Legacy single-file helpers
    # =================================================================

    def _load_resources_from_file(self) -> None:
        """Load resources from a single YAML file (legacy mode)."""
        resources_path = self.config_dir / self.resources_yaml

        if not resources_path.exists():
            raise FileNotFoundError(f"Resources config not found: {resources_path}")

        self.resources = ResourceBuilder.get_resources(
            str(resources_path), environment=self.environment
        )
        dg.get_dagster_logger().info(
            f"✅ Loaded {len(self.resources)} resources from {self.resources_yaml}"
        )

    def _load_from_file(self, config_file: str) -> Tuple[List, List, List, List]:
        """Parse a single pipeline YAML and build Dagster objects."""
        config_path = self.config_dir / config_file

        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")

        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)

        return self._build_from_dict(config_dict)

    # =================================================================
    # Shared builders
    # =================================================================

    def _build_resources(
        self, resource_dicts: list[dict[str, Any]]
    ) -> Dict[str, dg.ResourceDefinition]:
        """Build resource definitions and apply env overrides + noop IO."""
        resources = ResourceBuilder.build_resources_from_list(resource_dicts)

        if self.environment == "prod":
            resources = ResourceBuilder._apply_env_overrides(resources)

        from framework.io.noop import noop_io_manager

        resources["noop_io_manager"] = noop_io_manager

        return resources

    def _build_from_dict(
        self, config_dict: Dict[str, Any]
    ) -> Tuple[List, List, List, List]:
        """Build assets / jobs / sensors / checks from a raw config dict."""
        try:
            pipeline_config = FrameworkPipelineConfig(**config_dict)
        except ValidationError as e:
            raise ValueError(f"Configuration validation failed: {e}") from e

        return self._build_from_config(pipeline_config)

    def _build_from_config(
        self, config: FrameworkPipelineConfig
    ) -> Tuple[List, List, List, List]:
        """Build all Dagster objects from a validated pipeline config."""
        # Build the file_formatters lookup
        fmt_registry = {f.name: f for f in config.file_formatters}

        assets = AssetBuilder.build_assets(
            config.assets,
            jobs_config=config.jobs,
            file_formatters=fmt_registry or None,
        )
        jobs = JobBuilder.build_jobs(config.jobs)
        sensors = SensorBuilder.build_sensors(config.sensors)
        asset_checks = ValidationCheckBuilder.build_checks(config.assets)

        # ── CDC: auto-generate sensors + resources for change-tracked assets ──
        cdc_sensors, cdc_resources = CDCBuilder.build(config.assets)
        if cdc_sensors:
            sensors.extend(cdc_sensors)
            self.resources.update(cdc_resources)
            dg.get_dagster_logger().info(
                f"✅ CDC: built {len(cdc_sensors)} sensor(s), "
                f"{len(cdc_resources)} dispatcher resource(s)"
            )

        return assets, jobs, sensors, asset_checks

    # =================================================================
    # Public API
    # =================================================================

    def get_definitions(self, config_file: str | None = None) -> dg.Definitions:
        """Return a complete ``dg.Definitions``.

        Parameters
        ----------
        config_file:
            *Legacy only* — name of a single pipeline YAML file inside
            ``config_dir``.  When omitted the definitions are built from
            the already-discovered ``.macro`` files.

        Returns
        -------
        ``dg.Definitions`` ready to be consumed by Dagster.
        """
        if config_file is not None:
            # Legacy single-file path
            assets, jobs, sensors, asset_checks = self._load_from_file(config_file)
        elif self._pipeline_config is not None:
            # Discovery path — already loaded at init
            assets, jobs, sensors, asset_checks = self._build_from_config(
                self._pipeline_config
            )
        else:
            raise RuntimeError(
                "No configuration loaded. Either pass config_file or "
                "ensure .macro files exist in config_dir."
            )

        return dg.Definitions(
            assets=assets,
            asset_checks=asset_checks,
            jobs=jobs,
            sensors=sensors,
            resources=self.resources,
        )

    # =================================================================
    # Describe helpers
    # =================================================================

    def describe_resources(self) -> str:
        """Human-readable description of loaded resources."""
        if not self.resources:
            return "\n🔌 No resources loaded"

        lines = [
            f"\n🔌 Loaded Resources ({len(self.resources)}):",
            f"   Environment: {self.environment}",
        ]
        for name in sorted(self.resources.keys()):
            lines.append(f"   - {name}")
        return "\n".join(lines)

    def describe_config(self, config_file: str) -> str:
        """Human-readable description of a single pipeline config file."""
        config_path = self.config_dir / config_file

        with open(config_path, "r") as f:
            config_dict = yaml.safe_load(f)

        pipeline_config = FrameworkPipelineConfig(**config_dict)

        lines = [
            f"\n📊 Framework Configuration: {pipeline_config.name}",
            f"   Description: {pipeline_config.description or 'N/A'}",
            f"   Environment: {self.environment}",
        ]

        if self.resources:
            lines.append(f"\n🔌 Resources ({len(self.resources)}):")
            for name in sorted(self.resources.keys()):
                lines.append(f"   - {name}")

        lines.append(f"\n📊 Assets ({len(pipeline_config.assets)}):")
        for asset in pipeline_config.assets:
            lines.append(f"   - {asset.name} ({asset.type})")
            if asset.depends_on:
                lines.append(f"     Depends on: {', '.join(asset.depends_on)}")

        lines.append(f"\n⚙️  Jobs ({len(pipeline_config.jobs)}):")
        for job in pipeline_config.jobs:
            lines.append(f"   - {job.name}")
            lines.append(f"     Flow: {job.flow.definition}")

        lines.append(f"\n📡 Sensors ({len(pipeline_config.sensors)}):")
        for sensor in pipeline_config.sensors:
            lines.append(f"   - {sensor.name} ({sensor.type})")

        return "\n".join(lines)
