from typing import Optional, Type, TypeVar

import yaml
from pydantic import parse_obj_as
from pydantic.dataclasses import dataclass
from smart_open import open

from metaphor.common.api_sink import ApiSinkConfig
from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.file_sink import FileSinkConfig
from metaphor.common.variable import variable_substitution

# Create a generic variable that can be 'BaseConfig', or any subclass.
T = TypeVar("T", bound="BaseConfig")


@dataclass(config=ConnectorConfig)
class OutputConfig:
    """Config for where to output the data"""

    api: Optional[ApiSinkConfig] = None
    file: Optional[FileSinkConfig] = None


@dataclass(config=ConnectorConfig)
class BaseConfig:
    """Base class for runtime parameters

    All subclasses should add the @dataclass decorators
    """

    output: OutputConfig

    @classmethod
    def from_yaml_file(cls: Type[T], path: str) -> T:
        with open(path, encoding="utf8") as fin:
            obj = yaml.safe_load(fin.read())
            return parse_obj_as(cls, variable_substitution(obj))
