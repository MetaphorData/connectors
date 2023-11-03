class ConnectorConfig:
    """
    Pydantic dataclass Config for metaphor connector configurations
    """

    validate_default = True
    extra = "forbid"
    coerce_numbers_to_str = True
