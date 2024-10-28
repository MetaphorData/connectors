from typing import Optional

from pydantic import DirectoryPath
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class GreatExpectationConfig(BaseConfig):
    project_root_dir: DirectoryPath
    snowflake_account: Optional[str] = None
