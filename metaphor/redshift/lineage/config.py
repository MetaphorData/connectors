from pydantic.dataclasses import dataclass
from serde import deserialize

from metaphor.redshift.config import RedshiftRunConfig


@deserialize
@dataclass
class RedshiftLineageRunConfig(RedshiftRunConfig):

    # Whether to enable parsing view query to find upstream of the view, default True
    enable_view_lineage: bool = True

    # Whether to enable parsing stl_scan table to find table lineage information, default True
    enable_lineage_from_stl_scan: bool = True
