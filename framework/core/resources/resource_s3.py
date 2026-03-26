# resource_config.py
import dagster as dg

from framework.core.resources import resource_handler
from framework.model.resource_models import ResourceType


@resource_handler(ResourceType.s3)
def build_s3_resource(name: str, config: dict) -> dg.ResourceDefinition:

    @dg.resource
    def s3_resource(context):
        configs = {}

        yield configs

    return s3_resource