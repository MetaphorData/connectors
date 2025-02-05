from datetime import datetime
from unittest.mock import MagicMock, patch

import pytz
from google.cloud import bigquery

from metaphor.bigquery.job_change_event import JobChangeEvent
from metaphor.bigquery.utils import BigQueryResource
from tests.bigquery.load_entries import load_entries


def test_parse_log(test_root_dir):
    logs = load_entries(test_root_dir + "/bigquery/data/sample_log.json")

    results = [JobChangeEvent.from_entry(log) for log in logs]

    assert results == [
        JobChangeEvent(
            job_name="projects/metaphor-data/jobs/bquxjob_616d0f38_17e9c8d8782",
            job_type="COPY",
            timestamp=datetime(2022, 1, 27, 17, 20, 29, 105490, pytz.UTC),
            start_time=datetime(2022, 1, 27, 17, 20, 28, 366000, pytz.UTC),
            end_time=datetime(2022, 1, 27, 17, 20, 29, 93000, pytz.UTC),
            user_email="yi@metaphor.io",
            query=None,
            query_truncated=None,
            statementType=None,
            source_tables=[
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests1"
                )
            ],
            destination_table=BigQueryResource(
                project_id="metaphor-data", dataset_id="test", table_id="yi_tests2"
            ),
            default_dataset=None,
            input_bytes=None,
            output_bytes=None,
            output_rows=None,
        ),
        JobChangeEvent(
            job_name="projects/metaphor-data/jobs/7526798f-8072-446d-bdf1-ac1acb4d8591",
            job_type="QUERY",
            timestamp=datetime(2022, 1, 27, 17, 17, 12, 636773, pytz.UTC),
            start_time=datetime(2022, 1, 27, 17, 17, 11, 823000, pytz.UTC),
            end_time=datetime(2022, 1, 27, 17, 17, 12, 630000, pytz.UTC),
            user_email="bigquery-crawler@metaphor-data.iam.gserviceaccount.com",
            query="INSERT INTO `metaphor-data.test.yi_test3` \nSELECT * from `metaphor-data.test.yi_tests1` \nUNION ALL \nSELECT * from `metaphor-data.test.yi_tests2`",
            query_truncated=True,
            statementType="SELECT",
            source_tables=[
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests1"
                ),
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests2"
                ),
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests3"
                ),
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests"
                ),
            ],
            destination_table=BigQueryResource(
                project_id="metaphor-data", dataset_id="test", table_id="yi_tests"
            ),
            default_dataset=None,
            input_bytes=52781,
            output_bytes=None,
            output_rows=1,
        ),
    ]


@patch("google.cloud.bigquery.Client")
def test_fetch_job_query(mock_client: MagicMock):
    query = "SELECT * FROM my-project.dataset.table"
    mock_client.get_job = lambda job_id, project: bigquery.QueryJob(
        job_id=job_id,
        query=query,
        client=mock_client,
    )
    assert (
        JobChangeEvent._fetch_job_query(mock_client, "projects/my-project/jobs/1234")
        == query
    )


@patch("google.cloud.bigquery.Client")
def test_fetch_job_query_fail(mock_client: MagicMock):
    def fail_get_job(job_id, project):
        raise ValueError

    mock_client.get_job = fail_get_job
    assert not JobChangeEvent._fetch_job_query(
        mock_client, "projects/my-project/jobs/1234"
    )


def test_fetch_not_a_job():
    assert not JobChangeEvent._fetch_job_query(MagicMock(), "projects/my-project/jobs")
