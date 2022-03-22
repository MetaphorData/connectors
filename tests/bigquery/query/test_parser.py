from datetime import datetime

import pytz

from metaphor.bigquery.query.extractor import JobChangeEvent
from metaphor.bigquery.utils import BigQueryResource
from tests.bigquery.load_entries import load_entries


def test_parse_log(test_root_dir):
    logs = load_entries(test_root_dir + "/bigquery/query/data/sample_log.json")

    results = [JobChangeEvent.from_entry(log) for log in logs]

    assert results == [
        JobChangeEvent(
            timestamp=datetime(2022, 3, 4, 23, 40, 56, 595154, pytz.UTC),
            user_email="mars@metaphor.io",
            query="select * from `metaphor-data.test.yi_tests2`",
            statementType="SELECT",
            queried_tables=[
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_tests2"
                ),
            ],
        ),
        JobChangeEvent(
            timestamp=datetime(2022, 3, 7, 10, 29, 1, 625893, pytz.UTC),
            user_email="scott@metaphor.io",
            query="select `id`,\r\n    `name`\r\nfrom `metaphor-data`.`test`.`yi_test_view1`\r\nLIMIT 200 OFFSET 0",
            statementType="SELECT",
            queried_tables=[
                BigQueryResource(
                    project_id="metaphor-data", dataset_id="test", table_id="yi_test3"
                ),
                BigQueryResource(
                    project_id="metaphor-data",
                    dataset_id="test",
                    table_id="yi_test_view1",
                ),
            ],
        ),
    ]
