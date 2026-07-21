"""Dagster resource handler for Snowflake connections."""

import dagster as dg

from framework.core.resources import resource_handler
from framework.model.resource_models import ResourceType


@resource_handler(ResourceType.snowflake)
def build_snowflake_resource(name: str, config: dict) -> dg.ResourceDefinition:

    @dg.resource(required_resource_keys={"vault"} if "secret_path" in config else None)
    def snowflake_resource(context):
        try:
            import snowflake.connector
        except ImportError:
            raise ImportError(
                "snowflake-connector-python is required for Snowflake resources. "
                "Install with: pip install 'dagster-config-framework[snowflake]'"
            ) from None

        if "secret_path" in config:
            secrets = context.resources.vault.read_secret(
                secret_path=config["secret_path"]
            )
            user = secrets["username"]
            password = secrets["password"]
        else:
            user = config["user"]
            password = config["password"]

        conn = snowflake.connector.connect(
            account=config["account"],
            user=user,
            password=password,
            warehouse=config.get("warehouse"),
            database=config.get("database"),
            schema=config.get("schema"),
            role=config.get("role"),
        )

        context.log.info(f"Snowflake resource '{name}' connected")
        try:
            yield conn
        finally:
            conn.close()

    return snowflake_resource
