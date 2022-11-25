from typing import Optional

import yaml
from pydantic import parse_obj_as
from pydantic.dataclasses import dataclass
from smart_open import open

from metaphor.common.api_sink import ApiSinkConfig
from metaphor.common.file_sink import FileSinkConfig
from metaphor.common.variable import variable_substitution


@dataclass
class OutputConfig:
    """Config for where to output the data"""

    api: Optional[ApiSinkConfig] = None
    file: Optional[FileSinkConfig] = None


@dataclass
class BaseConfig:
    """Base class for runtime parameters

    All subclasses should add the @dataclass decorators
    """

    output: OutputConfig

    @classmethod
    def from_yaml_file(cls, path: str) -> "BaseConfig":
        with open(path, encoding="utf8") as fin:
            obj = yaml.safe_load(fin.read())
            return parse_obj_as(cls, variable_substitution(obj))
