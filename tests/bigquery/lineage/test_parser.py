from datetime import datetime

import pytz

from metaphor.bigquery.lineage.extractor import JobChangeEvent
from metaphor.bigquery.utils import BigQueryResource
from tests.bigquery.load_entries import load_entries


def test_parse_log(test_root_dir):
    logs = load_entries(test_root_dir + "/bigquery/lineage/data/sample_log.json")

    results = [JobChangeEvent.from_entry(log) for log in logs]

    assert results == [
        JobChangeEvent(
            job_name="projects/metaphor-data/jobs/bquxjob_616d0f38_17e9c8d8782",
            timestamp=datetime(2022, 1, 27, 17, 20, 29, 105490, pytz.UTC),
            user_email="yi@metaphor.io",
            query=None,
            statementType=None,
            source_tables=[
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests1"
                )
            ],
            destination_table=BigQueryResource(
                project_id="metaphor-data", dataset_id="test", table_id="yi_tests2"
            ),
        ),
        JobChangeEvent(
            job_name="projects/metaphor-data/jobs/7526798f-8072-446d-bdf1-ac1acb4d8591",
            timestamp=datetime(2022, 1, 27, 17, 17, 12, 636773, pytz.UTC),
            user_email="bigquery-crawler@metaphor-data.iam.gserviceaccount.com",
            query="INSERT INTO `metaphor-data.test.yi_test3` \nSELECT * from `metaphor-data.test.yi_tests1` \nUNION ALL \nSELECT * from `metaphor-data.test.yi_tests2`",
            statementType="SELECT",
            source_tables=[
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests1"
                ),
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests2"
                ),
            ],
            destination_table=BigQueryResource(
                project_id="metaphor-data", dataset_id="test", table_id="yi_tests3"
            ),
        ),
    ]
