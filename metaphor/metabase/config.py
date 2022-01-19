from dataclasses import dataclass

from serde import deserialize

from metaphor.common.extractor import RunConfig


@deserialize
@dataclass
class MetabaseRunConfig(RunConfig):
    server_url: str
    username: str
    password: str
