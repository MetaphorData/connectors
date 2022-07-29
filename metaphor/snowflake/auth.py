from typing import Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from pydantic import root_validator
from pydantic.dataclasses import dataclass
from smart_open import open

from metaphor.common.base_config import BaseConfig
from metaphor.common.utils import must_set_exactly_one

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

    # role to use when opening a connection
    role: Optional[str] = None

    # database context when opening a connection
    default_database: Optional[str] = None

    # the query tags for each snowflake query the connector issues
    query_tag: Optional[str] = METAPHOR_DATA

    @root_validator
    def have_password_or_private_key(cls, values):
        must_set_exactly_one(values, ["password", "private_key"])
        return values


def connect(config: SnowflakeAuthConfig) -> snowflake.connector.SnowflakeConnection:
    # key pair authentication
    pkb = None
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
        password=config.password,
        private_key=pkb,
        role=config.role,
        database=config.default_database,
        application=METAPHOR_DATA_SNOWFLAKE_CONNECTOR,
        session_parameters={
            "QUERY_TAG": config.query_tag,
            "quoted_identifiers_ignore_case": False,
        },
    )
