from dataclasses import field

from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.postgresql.config import PostgreSQLRunConfig


@deserialize
@dataclass
class RedshiftRunConfig(PostgreSQLRunConfig):
    port: int = 5439

    # While there's no explicit documentation on Redshift's system databases, they can be dervied from
    # https://docs.aws.amazon.com/redshift/latest/dg/r_DROP_DATABASE.html
    ignored_dbs: set[str] = field(
        default_factory=lambda: set(["dev", "padb_harvest", "template0", "template1"])
    )
