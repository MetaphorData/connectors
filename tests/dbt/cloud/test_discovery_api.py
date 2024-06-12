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
        elif json["query"].strip().startswith("query Test"):
            if json["variables"].get("uniqueId", "") == "bad test":
                return Response(response={"job": {"test": None}})
            return Response(
                response={
                    "job": {
                        "test": {
                            "status": "pass",
                        }
                    }
                }
            )
        assert False

    mock_requests_post.side_effect = fake_post
    discovery_api = DiscoveryAPI("url", "token")
    assert discovery_api.get_model_dataset_name(123, "foo") == "db.sch.tab"
    assert discovery_api.get_test_status(123, "foo") == "pass"
    assert discovery_api.get_test_status(123, "bad test") == "skipped"
