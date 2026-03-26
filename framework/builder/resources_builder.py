from pathlib import Path
from typing import Dict

import dagster as dg
import yaml

from framework.core.resources import RESOURCE_REGISTRY
from framework.io.noop import noop_io_manager
from framework.model.resource_models import ResourceConfig, ResourceType


class ResourceBuilder:
    @staticmethod
    def build_resources_from_yaml(path: str) -> Dict[str, dg.ResourceDefinition]:
        raw = yaml.safe_load(Path(path).read_text())

        return ResourceBuilder.build_resources_from_list(
            raw.get("resources", [])
        )

    @staticmethod
    def build_resources_from_list(
        resource_dicts: list[dict],
    ) -> Dict[str, dg.ResourceDefinition]:
        """Build resource definitions from a list of raw resource dicts.

        Parameters
        ----------
        resource_dicts:
            List of resource config dicts (each must have ``name``, ``type``,
            and optionally ``config``).

        Returns
        -------
        Dict mapping resource name → ``ResourceDefinition``.
        """
        resource_models = {
            r["name"]: ResourceConfig(**r) for r in resource_dicts
        }

        resources: Dict[str, dg.ResourceDefinition] = {}

        # Phase 1: build vault resources first
        for name, res in resource_models.items():
            if res.type == ResourceType.vault:
                handler = RESOURCE_REGISTRY.get(res.type)
                resources[name] = handler(name, res.config)

        # Phase 2: build remaining resources
        for name, res in resource_models.items():
            if res.type == ResourceType.vault:
                continue

            handler = RESOURCE_REGISTRY.get(res.type)
            resources[name] = handler(name, res.config)

        return resources

    @staticmethod
    def get_resources(
        resources_yaml_path: str, environment: str = "local"
    ) -> Dict[str, dg.ResourceDefinition]:
        """
        Load resources from YAML and optionally override with environment values.

        Args:
            resources_yaml_path: Path to resources.yaml
            environment: "local" or "prod"

        Returns:
            Dictionary of configured resources
        """
        resources = ResourceBuilder.build_resources_from_yaml(resources_yaml_path)

        # Override with environment variables if in production
        if environment == "prod":
            resources = ResourceBuilder._apply_env_overrides(resources)

        resources["noop_io_manager"] = noop_io_manager

        return resources

    @staticmethod
    def _apply_env_overrides(
        resources: Dict[str, dg.ResourceDefinition],
    ) -> Dict[str, dg.ResourceDefinition]:
        """Override resource config with environment variables for production"""
        # This is a placeholder for environment variable overrides
        # Can be extended based on needs
        return resources
