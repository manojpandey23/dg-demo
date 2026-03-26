# resource_vault.py
from dagster_hashicorp.vault import vault_resource
import dagster as dg

from framework.core.resources import resource_handler
from framework.model.resource_models import ResourceType



@resource_handler(ResourceType.vault)
def build_vault_resource(name: str, config: dict) -> dg.ResourceDefinition:
    # config is passed directly to Dagster's vault_resource
    return vault_resource.configured(config)