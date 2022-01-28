from metaphor.common.base_config import OutputConfig
from metaphor.common.filter import DatasetFilter
from metaphor.common.sampling import SamplingConfig
from metaphor.postgresql.profile.config import PostgreSQLProfileRunConfig


def test_yaml_config(test_root_dir):
    config = PostgreSQLProfileRunConfig.from_yaml_file(
        f"{test_root_dir}/postgresql/profile/config.yml"
    )

    assert config == PostgreSQLProfileRunConfig(
        host="host",
        database="database",
        user="user",
        password="password",
        filter=DatasetFilter(
            includes={
                "db1": {
                    "schema1": None,
                },
            },
            excludes={"db3": None},
        ),
        sampling=SamplingConfig(percentage=10),
        port=1234,
        output=OutputConfig(),
    )
