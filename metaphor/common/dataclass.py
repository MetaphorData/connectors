from pydantic import ConfigDict

ConnectorConfig = ConfigDict(
    validate_default=True, extra="forbid", coerce_numbers_to_str=True
)
