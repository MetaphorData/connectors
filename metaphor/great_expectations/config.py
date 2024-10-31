from typing import Optional

from pydantic import DirectoryPath
from pydantic.dataclasses import dataclass

from metaphor.common.base_config import BaseConfig
from metaphor.common.dataclass import ConnectorConfig


@dataclass(config=ConnectorConfig)
class GreatExpectationConfig(BaseConfig):
    project_root_dir: DirectoryPath
    """
    The project root directory. This is the directory that contains your Great Expectations artifacts, i.e. where the `gx` directory lives.
    """

    snowflake_account: Optional[str] = None
    """
    The Snowflake account to use if the Great Expectations run was targeted at Snowflake.
    """
