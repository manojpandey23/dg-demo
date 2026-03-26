# resource_api.py
import requests
import dagster as dg

from framework.core.resources import resource_handler
from framework.model.resource_models import ResourceType


@resource_handler(ResourceType.api)
def build_api_resource(name: str, config: dict) -> dg.ResourceDefinition:
    base_url = config.get("base_url", "http://localhost:8000")
    timeout = config.get("timeout", 10)
    headers = config.get("headers", {})

    @dg.resource
    def api_resource(context):
        session = requests.Session()
        session.base_url = base_url
        session.timeout = timeout
        if headers:
            session.headers.update(headers)

        context.log.info(f"✅ API Resource '{name}' initialized: {base_url}")
        yield session
        session.close()

    return api_resource
