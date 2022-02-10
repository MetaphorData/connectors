from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.redshift.config import RedshiftRunConfig


@deserialize
@dataclass
class RedshiftLineageRunConfig(RedshiftRunConfig):

    # Whether to enable parsing stl_scan table to find table lineage information, default True
    enable_lineage_from_stl_scan: bool = True
