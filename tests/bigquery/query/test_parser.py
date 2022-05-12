from datetime import datetime

import pytz

from metaphor.bigquery.logEvent import JobChangeEvent
from metaphor.bigquery.utils import BigQueryResource
from tests.bigquery.load_entries import load_entries


def test_parse_log(test_root_dir):
    logs = load_entries(test_root_dir + "/bigquery/query/data/sample_log.json")

    results = [JobChangeEvent.from_entry(log) for log in logs]

    assert results == [
        JobChangeEvent(
            job_name="projects/metaphor-data/jobs/bquxjob_70ddc06_17f574eca64",
            timestamp=datetime(2022, 3, 4, 23, 40, 56, 595154, pytz.UTC),
            user_email="mars@metaphor.io",
            query="select * from `metaphor-data.test.yi_tests2`",
            statementType="SELECT",
            source_tables=[
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests2"
                ),
            ],
            destination_table=BigQueryResource(
                project_id="metaphor-data",
                dataset_id="3198487640cceeb8e7d28d7c0f23e9f51ed519b1",
                table_id="anon66e97db94a52893f2f3ee8e5c3d420963d37c90c",
            ),
        ),
        JobChangeEvent(
            job_name="projects/metaphor-data/jobs/job_BSqXkGDLhaGKKQDJtesGJt3gjwG5",
            timestamp=datetime(2022, 3, 7, 10, 29, 1, 625893, pytz.UTC),
            user_email="scott@metaphor.io",
            query="select `id`,\r\n    `name`\r\nfrom `metaphor-data`.`test`.`yi_test_view1`\r\nLIMIT 200 OFFSET 0",
            statementType="SELECT",
            source_tables=[
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_test3"
                ),
                BigQueryResource(
                    project_id="metaphor-data",
                    dataset_id="test",
                    table_id="yi_test_view1",
                ),
            ],
            destination_table=BigQueryResource(
                project_id="metaphor-data",
                dataset_id="7920ed6988b5b158b0bcde435ea325d866993be9",
                table_id="anon8d8bdc7387b1ebe499e35632b1dd94a6232badf3",
            ),
        ),
    ]
