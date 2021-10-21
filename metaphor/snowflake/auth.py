from dataclasses import dataclass
from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import dsa, rsa
from serde import deserialize
from smart_open import open
from snowflake import connector

from metaphor.common.extractor import RunConfig


@deserialize
@dataclass
class SnowflakeAuthConfig(RunConfig):
    account: str
    user: str

    # if using default authenticator
    password: Optional[str] = None

    # if using key pair authentication
    private_key_file: Optional[str] = None
    # if private key is encrypted
    private_key_passphrase: Optional[str] = None

    # database context when opening a connection
    default_database: Optional[str] = None


def connect(config: SnowflakeAuthConfig) -> connector.SnowflakeConnection:
    # default authenticator
    if config.password is not None:
        return connector.connect(
            account=config.account,
            user=config.user,
            password=config.password,
            database=config.default_database,
        )

    # key pair authentication
    if config.private_key_file is not None:
        passphrase = (
            config.private_key_passphrase.encode()
            if config.private_key_passphrase is not None
            else None
        )

        with open(config.private_key_file, "rb") as f:
            p_key = serialization.load_pem_private_key(
                f.read(), password=passphrase, backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return connector.connect(
            account=config.account,
            user=config.user,
            private_key=pkb,
            database=config.default_database,
        )

    raise ValueError("Invalid Snowflake configuration, no authentication specified")
