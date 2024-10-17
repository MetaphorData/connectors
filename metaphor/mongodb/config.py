from typing import Optional, Set

from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass
from pymongo import MongoClient
from pymongo.auth import MECHANISMS

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class MongoDBConfig(BaseConfig):
    uri: str
    auth_mechanism: str = "DEFAULT"
    tls: bool = True

    infer_schema_sample_size: Optional[int] = 1000
    excluded_databases: Set[str] = Field(default_factory=set)
    excluded_collections: Set[str] = Field(default_factory=set)

    @field_validator("auth_mechanism", mode="before")
    def _validate_auth_mechanism(cls, auth_mechanism: str):
        if auth_mechanism not in MECHANISMS:
            raise ValueError(
                f"Invalid auth mechanism specified, valid values: {MECHANISMS}"
            )
        return auth_mechanism

    def get_client(self):
        kwargs = {
            "authMechanism": self.auth_mechanism,
            "tls": self.tls,
            "readPreference": "primary",
            "w": "majority",
        }
        if self.auth_mechanism == "MONGODB-AWS":
            kwargs["authSource"] = "$external"

        return MongoClient(
            self.uri,
            **kwargs,
        )
