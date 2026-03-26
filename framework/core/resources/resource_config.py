# resource_config.py
import yaml
import dagster as dg
from pathlib import Path

from framework.core.resources import resource_handler
from framework.model.resource_models import ResourceType


@resource_handler(ResourceType.config)
def build_config_resource(name: str, config: dict) -> dg.ResourceDefinition:
    config_dir = config.get("config_dir", "src/price_domain/configs")

    @dg.resource
    def config_resource(context):
        configs = {}
        path = Path(config_dir)

        for file in path.glob("*.yaml"):
            if file.name == "resources.yaml":
                continue
            with open(file, "r") as f:
                configs[file.name] = yaml.safe_load(f)

        context.log.info(
            f"✅ Config Resource '{name}' loaded {len(configs)} files"
        )
        yield configs

    return config_resource