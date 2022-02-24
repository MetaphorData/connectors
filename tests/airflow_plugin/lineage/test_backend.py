import tempfile
from datetime import datetime
from unittest.mock import MagicMock, patch

from airflow import DAG
from airflow.models.baseoperator import BaseOperator
from freezegun import freeze_time

from metaphor.airflow_plugin.lineage.backend import (
    MetaphorBackend,
    MetaphorBackendConfig,
)
from metaphor.airflow_plugin.lineage.entity import DataPlatform, MetaphorDataset
from tests.test_utils import load_json


class OperatorWithSQL(BaseOperator):
    sql: str

    def __init__(self, sql: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.sql = sql


@freeze_time("2000-01-01")
@patch("metaphor.airflow_plugin.lineage.backend.MetaphorBackendConfig.from_config")
def test_metaphor_lineage_backend(mock_from_config: MagicMock):
    with tempfile.TemporaryDirectory() as temp_directory:
        mock_from_config.return_value = MetaphorBackendConfig(
            mode="s3",
            s3_url=temp_directory,
        )

        backend = MetaphorBackend()
        task_id = "task_id"
        dag_id = "dag_id"
        task = OperatorWithSQL(task_id=task_id, sql="select * from table")
        dag = DAG(dag_id=dag_id)
        timestamp = 1640995200
        lineage_name = f"{timestamp}_{dag_id}_{task_id}"

        context = {
            "task": task,
            "dag": dag,
            "execution_date": datetime.utcfromtimestamp(timestamp),
        }

        assert lineage_name == MetaphorBackend._populate_lineage_name(context)

        backend.send_lineage(
            operator=task,
            inlets=[
                MetaphorDataset(
                    database="metaphor",
                    schema="public",
                    table="in",
                    platform=DataPlatform.REDSHIFT,
                ),
            ],
            outlets=[
                MetaphorDataset(
                    database="metaphor",
                    schema="public",
                    table="out",
                    platform=DataPlatform.REDSHIFT,
                ),
            ],
            context=context,
        )

        assert [
            {
                "dataset": {
                    "entityType": "DATASET",
                    "logicalId": {
                        "name": "metaphor.public.out",
                        "platform": "REDSHIFT",
                    },
                    "upstream": {
                        "entityId": "",
                        "latest": False,
                        "sourceDatasets": ["DATASET~A100403449D6C7F9B07F93D13D7CE873"],
                        "transformation": "select * from table",
                    },
                },
                "eventHeader": {
                    "appName": "",
                    "server": "",
                    "time": "2000-01-01T00:00:00+00:00",
                },
            },
        ] == load_json(f"{temp_directory}/{lineage_name}/1-of-1.json")
