"""Dagster resource handler for AWS S3 (boto3 session)."""

import dagster as dg

from framework.core.resources import resource_handler
from framework.model.resource_models import ResourceType


@resource_handler(ResourceType.s3)
def build_s3_resource(name: str, config: dict) -> dg.ResourceDefinition:

    @dg.resource(required_resource_keys={"vault"} if "secret_path" in config else None)
    def s3_resource(context):
        try:
            import boto3
        except ImportError:
            raise ImportError(
                "boto3 is required for S3 resources. "
                "Install with: pip install 'dagster-config-framework[s3]'"
            ) from None

        if "secret_path" in config:
            secrets = context.resources.vault.read_secret(
                secret_path=config["secret_path"]
            )
            session = boto3.Session(
                aws_access_key_id=secrets["access_key"],
                aws_secret_access_key=secrets["secret_key"],
                region_name=config.get("region", "us-east-1"),
            )
        elif config.get("profile"):
            session = boto3.Session(
                profile_name=config["profile"],
                region_name=config.get("region", "us-east-1"),
            )
        elif config.get("access_key"):
            session = boto3.Session(
                aws_access_key_id=config["access_key"],
                aws_secret_access_key=config["secret_key"],
                region_name=config.get("region", "us-east-1"),
            )
        else:
            session = boto3.Session(
                region_name=config.get("region", "us-east-1"),
            )

        client = session.client(
            "s3",
            endpoint_url=config.get("endpoint_url"),
        )

        context.log.info(
            f"S3 resource '{name}' created "
            f"(region={config.get('region', 'us-east-1')}, "
            f"profile={config.get('profile', 'default')})"
        )

        yield {
            "session": session,
            "client": client,
            "bucket": config.get("bucket"),
        }

    return s3_resource
