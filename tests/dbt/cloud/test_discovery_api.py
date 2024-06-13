from dataclasses import dataclass
from typing import Any, Dict
from unittest.mock import MagicMock, patch

from metaphor.dbt.cloud.discovery_api import DiscoveryAPI


@patch("requests.post")
def test(mock_requests_post: MagicMock):
    def fake_post(
        url: str, headers: Dict[str, Any], json: Dict[str, Any], timeout: int
    ):
        @dataclass
        class Response:
            response: Dict[str, Any]

            def json(self):
                return {
                    "data": self.response,
                }

        if json["query"].strip().startswith("query Model"):
            return Response(
                response={
                    "job": {
                        "model": {
                            "alias": None,
                            "database": "db",
                            "schema": "sch",
                            "name": "tab",
                        }
                    }
                }
            )
        elif json["query"].strip().startswith("query Tests"):
            job_id = json["variables"].get("jobId", 0)
            if job_id == 0:
                return Response(response={"job": {"tests": []}})
            elif job_id == 1:
                return Response(
                    response={
                        "job": {
                            "tests": [
                                {
                                    "uniqueId": "1",
                                    "name": None,
                                    "columnName": "col",
                                    "status": "pass",
                                },
                                {
                                    "uniqueId": "2",
                                    "name": "not pass",
                                    "columnName": "col2",
                                    "status": "error",
                                },
                            ]
                        }
                    }
                )
        assert False

    mock_requests_post.side_effect = fake_post
    discovery_api = DiscoveryAPI("url", "token")
    assert discovery_api.get_model_dataset_name(123, "foo") == "db.sch.tab"
    assert not discovery_api.get_all_test_status(0)
    test_statuses = discovery_api.get_all_test_status(1)
    assert len(test_statuses) == 2
    assert test_statuses[0].name is None
    assert test_statuses[0].status == "pass"
    assert test_statuses[1].columnName == "col2"
    assert test_statuses[1].status == "error"
