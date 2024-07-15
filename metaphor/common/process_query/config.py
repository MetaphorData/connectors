from pydantic.dataclasses import dataclass

from metaphor.common.base_config import ConnectorConfig


@dataclass(config=ConnectorConfig)
class ProcessQueryConfig:
    """
    Config to control what to do when processing the parsed SQL queries.
    """

    redact_literal_values_in_where_clauses: bool = False
    """
    Whether to redact literal values in WHERE clauses. If set to `True`, all literal values will be redacted to a predefined string value.
    """

    redacted_literal_placeholder: str = "<REDACTED>"

    @property
    def should_process(self) -> bool:
        """
        Whether we should run the processing method at all.
        """
        return self.redact_literal_values_in_where_clauses
