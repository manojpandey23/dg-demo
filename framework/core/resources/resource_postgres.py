# resource_postgres.py
import dagster as dg
import psycopg2

from framework.core.resources import resource_handler
from framework.model.resource_models import ResourceType


@resource_handler(ResourceType.postgres)
def build_postgres_resource_vault(name: str, config: dict) -> dg.ResourceDefinition:
    host = config["host"]
    port = config.get("port", 5432)
    database = config["database"]

    @dg.resource(required_resource_keys={"vault"} if "secret_path" in config else None)
    def postgres_resource(context):
        if "secret_path" in config:
            secrets = context.resources.vault.read_secret(
                secret_path=config["secret_path"]
            )
            user = secrets["username"]
            password = secrets["password"]
        else:
            user = config["user"]
            password = config["password"]

        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
        )

        try:
            yield conn
        finally:
            conn.close()

    return postgres_resource
