from typing import Optional, Type, TypeVar

import yaml
from pydantic import TypeAdapter
from pydantic.dataclasses import dataclass
from smart_open import open

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.file_sink import FileSinkConfig
from metaphor.common.variable import variable_substitution

# Create a generic variable that can be 'BaseConfig', or any subclass.
T = TypeVar("T", bound="BaseConfig")


@dataclass(config=ConnectorConfig)
class OutputConfig:
    """Config for where to output the data"""

    file: Optional[FileSinkConfig] = None


@dataclass()
class BaseConfig:
    """Base class for runtime parameters

    All subclasses should add the @dataclass decorators
    """

    output: OutputConfig

    @classmethod
    def from_yaml_file(cls: Type[T], path: str) -> T:
        with open(path, encoding="utf8") as fin:
            obj = yaml.safe_load(fin.read())
            return TypeAdapter(cls).validate_python(variable_substitution(obj))
