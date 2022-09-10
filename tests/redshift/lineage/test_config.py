from metaphor.common.base_config import OutputConfig
from metaphor.redshift.lineage.config import RedshiftLineageRunConfig


def test_yaml_config(test_root_dir):
    config = RedshiftLineageRunConfig.from_yaml_file(
        f"{test_root_dir}/redshift/lineage/config.yml"
    )

    assert config == RedshiftLineageRunConfig(  # nosec
        host="host",
        database="database",
        user="user",
        password="password",
        enable_view_lineage=False,
        enable_lineage_from_stl_scan=False,
        include_self_lineage=False,
        output=OutputConfig(),
    )
