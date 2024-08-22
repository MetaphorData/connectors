from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import ConnectorConfig


@dataclass(config=ConnectorConfig)
class RedactPIILiteralsConfig:
    """
    Config to control whether we want to redact literal values. Useful if you want to remove PII from your queries.
    """

    redact: bool = False

    placeholder_literal: str = "<REDACTED>"


@dataclass(config=ConnectorConfig)
class ProcessQueryConfig:
    """
    Config to control what to do when processing the parsed SQL queries.
    """

    redact_literals: RedactPIILiteralsConfig = field(
        default_factory=lambda: RedactPIILiteralsConfig()
    )

    ignore_insert_values_into: bool = False
    """
    Ignore `INSERT INTO ... VALUES` expressions. These expressions don't have any
    lineage information, and are often very large in size.
    """

    @property
    def should_process(self) -> bool:
        """
        Whether we should run the processing method at all.
        """
        return self.redact_literals.redact or self.ignore_insert_values_into
