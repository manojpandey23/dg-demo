# resource_logger.py
import dagster as dg

from framework.core.resources import resource_handler
from framework.model.resource_models import ResourceType


@resource_handler(ResourceType.logger)
def build_logger_resource(name: str, config: dict) -> dg.ResourceDefinition:
    @dg.resource
    def logger_resource(context):
        return context.log
    return logger_resource
