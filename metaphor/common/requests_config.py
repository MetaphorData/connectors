from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class RequestsConfig:
    """
    Contains configuration values regarding HTTP requests.
    """

    timeout: int = 10
    """
    How many seconds before the requests client fails on a timed out request.
    """
