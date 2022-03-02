from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pydantic.dataclasses import dataclass
from smart_open import open

from metaphor.common.base_config import BaseConfig

try:
    import snowflake.connector
except ImportError:
    print("Please install metaphor[snowflake] extra\n")
    raise


METAPHOR_DATA = "MetaphorData"
METAPHOR_DATA_SNOWFLAKE_CONNECTOR = "MetaphorData_SnowflakeConnector"


@dataclass
class SnowflakeKeyPairAuthConfig:
    """Config for key pair authentication"""

    key_file: str

    # provide decryption passphrase if private key is encrypted
    passphrase: Optional[str] = None


@dataclass
class SnowflakeAuthConfig(BaseConfig):
    account: str
    user: str

    # if using default authenticator
    password: Optional[str] = None

    # if using key pair authentication
    private_key: Optional[SnowflakeKeyPairAuthConfig] = None

    # database context when opening a connection
    default_database: Optional[str] = None

    # the query tags for each snowflake query the connector issues
    query_tag: Optional[str] = METAPHOR_DATA


def connect(config: SnowflakeAuthConfig) -> snowflake.connector.SnowflakeConnection:
    # default authenticator
    if config.password is not None:
        return snowflake.connector.connect(
            account=config.account,
            user=config.user,
            password=config.password,
            database=config.default_database,
            application=METAPHOR_DATA_SNOWFLAKE_CONNECTOR,
            session_parameters={
                "QUERY_TAG": config.query_tag,
                "quoted_identifiers_ignore_case": False,
            },
        )

    # key pair authentication
    if config.private_key is not None:
        passphrase = (
            config.private_key.passphrase.encode()
            if config.private_key.passphrase is not None
            else None
        )

        with open(config.private_key.key_file, "rb") as f:
            p_key = serialization.load_pem_private_key(
                f.read(), password=passphrase, backend=default_backend()
            )

        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return snowflake.connector.connect(
            account=config.account,
            user=config.user,
            private_key=pkb,
            database=config.default_database,
            application=METAPHOR_DATA_SNOWFLAKE_CONNECTOR,
            session_parameters={
                "QUERY_TAG": config.query_tag,
            },
        )

    raise ValueError(
        "Invalid Snowflake configuration, please set either password or private key"
    )
