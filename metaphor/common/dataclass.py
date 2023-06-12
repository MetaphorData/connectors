from pydantic import Extra


class ConnectorConfig:
    """
    Pydantic dataclass Config for metaphor connector configurations
    """

    validate_all = True
    extra = Extra.forbid
