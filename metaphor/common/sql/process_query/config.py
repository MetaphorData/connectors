from __future__ import annotations
from dataclasses import field

from pydantic import Field, ValidationInfo, field_validator, model_validator
from pydantic.dataclasses import dataclass

from metaphor.common.logger import get_logger
from metaphor.common.base_config import ConnectorConfig

logger = get_logger()


@dataclass(config=ConnectorConfig)
class RedactPIILiteralsConfig:
    """
    Config to control whether we want to redact literal values. Useful if you want to remove PII from your queries.
    """

    where_clauses: bool = Field(default=False)
    """
    Deprecated.
    """

    case_clauses: bool = Field(default=False)
    """
    Deprecated.
    """

    when_not_matched_insert_clauses: bool = Field(default=False)
    """
    Deprecated.
    """

    redact: bool = False

    placeholder_literal: str = "<REDACTED>"

    @field_validator("where_clauses", "case_clauses", "when_not_matched_insert_clauses")
    @classmethod
    def _warn_deprecated_fields(cls, value: bool, info: ValidationInfo) -> bool:
        if value:
            logger.warning(f"Deprecated field set to True: {info.field_name}")
        return False


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
