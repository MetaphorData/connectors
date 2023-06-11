from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import DataclassConfig
from metaphor.redshift.config import RedshiftRunConfig


@dataclass(config=DataclassConfig)
class RedshiftLineageRunConfig(RedshiftRunConfig):
    # Whether to enable parsing view query to find upstream of the view, default True
    enable_view_lineage: bool = True

    # Whether to enable parsing stl_query table to find table lineage information, default True
    enable_lineage_from_sql: bool = True

    # Whether to include self loop in lineage
    include_self_lineage: bool = True
