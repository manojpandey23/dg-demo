# resource_http.py
import requests
import dagster as dg

from framework.core.resources import resource_handler
from framework.model.resource_models import ResourceType


@resource_handler(ResourceType.http)
def build_http_resource(name: str, config: dict) -> dg.ResourceDefinition:
    timeout = config.get("timeout", 30)
    verify_ssl = config.get("verify_ssl", True)

    @dg.resource
    def http_resource(context):
        session = requests.Session()
        session.timeout = timeout
        session.verify = verify_ssl
        yield session
        session.close()

    return http_resource