from enum import Enum

from pydantic import BaseModel, Field, model_validator
from typing import Optional, Dict, Any


class ResourceType(str, Enum):
    api = "api"
    ftp = "ftp"
    file_system = "file_system"
    event = "event"
    postgres = "postgres"
    snowflake = "snowflake"
    s3 = "s3"
    vault = "vault"
    config = "config"
    http = "http"
    logger = "logger"

class ResourceConfig(BaseModel):
    name: str
    type: ResourceType
    description: Optional[str] = None
    config: Dict[str, Any] = Field(default_factory=dict)
    credentials_from: Optional[str] = None


class VaultResourceConfig(BaseModel):
    url: str
    auth_type: Dict[str, Any]


class PostgresResourceConfig(BaseModel):
    host: str
    port: int = 5432
    database: str
    user: Optional[str] = None
    password: Optional[str] = None
    secret_path: Optional[str] = None

    @model_validator(mode="after")
    def validate_auth(self):
        if not (self.user and self.password) and not self.secret_path:
            raise ValueError(
                "Postgres requires either user/password or secret_path"
            )
        return self


class S3ResourceConfig(BaseModel):
    bucket: str
    region: str = "us-east-1"
    secret_path: Optional[str] = None
