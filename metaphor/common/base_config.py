from typing import Optional

from pydantic.dataclasses import dataclass
from serde import deserialize
from serde.yaml import from_yaml
from smart_open import open

from .api_sink import ApiSinkConfig
from .file_sink import FileSinkConfig


@deserialize
@dataclass
class OutputConfig:
    """Config for where to output the data"""

    api: Optional[ApiSinkConfig] = None
    file: Optional[FileSinkConfig] = None


@deserialize
@dataclass
class BaseConfig:
    """Base class for runtime parameters

    All subclasses should add the @dataclass @deserialize decorators
    """

    output: OutputConfig

    @classmethod
    def from_yaml_file(cls, path: str) -> "BaseConfig":
        with open(path, encoding="utf8") as fin:
            return from_yaml(cls, fin.read())
