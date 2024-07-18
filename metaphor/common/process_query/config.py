from dataclasses import field

from pydantic.dataclasses import dataclass

from metaphor.common.base_config import ConnectorConfig


@dataclass(config=ConnectorConfig)
class RedactLiteralConfig:
    """
    Config to control whether we want to redact literal values. Useful if you want to remove PII from your queries.
    """

    where_clauses: bool = False
    """
    Whether to redact literal values in WHERE clauses. If set to `True`, all literal values will be redacted to a predefined string value.
    """

    when_not_matched_insert_clauses: bool = False
    """
    Whether to redact literal values in WHEN NOT MATCHED INSERT clauses. If set to `True`, all literal values will be redacted to a predefined string value.
    """

    placeholder_literal: str = "<REDACTED>"


@dataclass(config=ConnectorConfig)
class ProcessQueryConfig:
    """
    Config to control what to do when processing the parsed SQL queries.
    """

    redact: RedactLiteralConfig = field(default_factory=lambda: RedactLiteralConfig())

    ignore_insert_values_into: bool = True
    """
    Ignore `INSERT INTO ... VALUES` expressions. These expressions don't have any
    lineage information, and are often very large in size.
    """

    @property
    def should_process(self) -> bool:
        """
        Whether we should run the processing method at all.
        """
        return (
            self.redact.where_clauses
            or self.redact.when_not_matched_insert_clauses
            or self.ignore_insert_values_into
        )
