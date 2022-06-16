from metaphor.common.base_config import OutputConfig
from metaphor.dbt.config import MetaOwnership, MetaTag
from metaphor.dbt.extractor import DbtRunConfig


def test_yaml_config(test_root_dir):
    config = DbtRunConfig.from_yaml_file(f"{test_root_dir}/dbt/config.yml")

    assert config == DbtRunConfig(
        manifest="manifest",
        catalog="catalog",
        docs_base_url="http://localhost",
        project_source_url="http://foo.bar",
        meta_ownerships=[
            MetaOwnership(
                meta_key="owner",
                email_domain="metaphor.io",
                ownership_type="Maintainer",
            )
        ],
        meta_tags=[
            MetaTag(
                meta_key="pii",
                meta_value_matcher="False",
                tag_type="no_pii",
            )
        ],
        output=OutputConfig(),
    )
