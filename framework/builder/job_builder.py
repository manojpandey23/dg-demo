"""
Dynamic job builder.

Generates Dagster jobs from JobConfig objects.
Parses flow definitions and creates asset-based jobs with proper orchestration.
"""

from __future__ import annotations

from typing import Dict, List, Optional

import dagster as dg

from framework.builder.asset_builder import AssetBuilder
from framework.builder.flow_parser import FlowParser
from framework.model.config_models import JobConfig


class JobBuilder:
    """Factory for building Dagster jobs from configuration."""

    _execution_orders: dict[str, list[str]] = {}

    @classmethod
    def reset(cls) -> None:
        """Clear shared state between runs (important for tests)."""
        cls._execution_orders = {}

    @staticmethod
    def build_jobs(configs: list[JobConfig]) -> list[dg.UnresolvedAssetJobDefinition]:
        JobBuilder.reset()
        jobs: list[dg.UnresolvedAssetJobDefinition] = []
        for config in configs:
            job = JobBuilder.build_job(config)
            if job is not None:
                jobs.append(job)
        return jobs

    @staticmethod
    def build_job(config: JobConfig) -> Optional[dg.UnresolvedAssetJobDefinition]:
        flow_def = config.flow.definition

        try:
            nodes = FlowParser.parse(flow_def)
            execution_order = FlowParser.get_execution_order(nodes)
        except Exception as e:
            raise ValueError(
                f"Job '{config.name}': Failed to parse flow '{flow_def}' — {e}"
            ) from e

        if not execution_order:
            return None

        # Base assets from flow
        asset_names = set(execution_order)

        # ✅ Inject OR‑bridges automatically
        or_bridge_map = AssetBuilder.get_or_bridge_map()
        for asset in list(asset_names):
            if asset in or_bridge_map:
                asset_names.add(or_bridge_map[asset])

        asset_keys = [dg.AssetKey(name) for name in sorted(asset_names)]
        asset_selection = dg.AssetSelection.keys(*asset_keys)

        tags = dict(config.tags or {})
        tags["flow"] = flow_def

        job = dg.define_asset_job(
            name=config.name,
            selection=asset_selection,
            description=config.description,
            tags=tags,
            config=config.config,
        )

        JobBuilder._execution_orders[config.name] = list(asset_names)
        return job

    @staticmethod
    def get_asset_order(job_name: str) -> List[str]:
        """
        Return the topologically sorted asset names for a previously built job.

        Args:
            job_name: Name of the job as specified in config.

        Returns:
            List of asset names in execution order.

        Raises:
            KeyError: If the job was not built through this builder.
        """
        if job_name not in JobBuilder._execution_orders:
            raise KeyError(
                f"No execution order cached for job '{job_name}'. "
                "Ensure the job was built via JobBuilder.build_job first."
            )
        return list(JobBuilder._execution_orders[job_name])
