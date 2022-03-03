from metaphor.models.metadata_change_event import DataPlatform, DatasetLogicalID

from metaphor.airflow_plugin.lineage.entity import MetaphorDataset


def test_metaphor_dataset():
    assert MetaphorDataset.to_dataset_logical_id(
        MetaphorDataset(
            database="metaphor",
            schema="PUBLIC",
            table="foo",
            platform=DataPlatform.REDSHIFT,
        )
    ) == DatasetLogicalID(
        account=None, name="metaphor.public.foo", platform=DataPlatform.REDSHIFT
    )

    assert MetaphorDataset.to_dataset_logical_id(
        MetaphorDataset(
            database="metaphor",
            schema="public",
            table="BAR",
            snowflake_account="dev@metaphor.io",
            platform=DataPlatform.SNOWFLAKE,
        )
    ) == DatasetLogicalID(
        account="dev@metaphor.io",
        name="metaphor.public.bar",
        platform=DataPlatform.SNOWFLAKE,
    )

    assert MetaphorDataset.to_dataset_logical_id(
        MetaphorDataset(
            database="project_id",
            schema="public",
            table="BAR",
            snowflake_account="dev@metaphor.io",
            platform=DataPlatform.BIGQUERY,
        )
    ) == DatasetLogicalID(
        account=None, name="project_id.public.bar", platform=DataPlatform.BIGQUERY
    )
