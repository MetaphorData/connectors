import json
from unittest.mock import MagicMock, patch

from requests import Response

from metaphor.alation.client import Client


@patch("requests.get")
def test_client(mock_requests_get: MagicMock) -> None:
    client = Client(
        base_url="http://alation-instance", headers={"Token": "woo an awesome token"}
    )
    resp1 = Response()
    resp1._content = json.dumps([{"id": x} for x in range(11)]).encode()
    resp1.status_code = 200
    resp1.headers = {"X-Next-Page": "http://bogus-url"}  # type: ignore
    resp2 = Response()
    resp2._content = json.dumps([{"id": x} for x in range(11, 18)]).encode()
    resp2.status_code = 200
    mock_requests_get.side_effect = [resp1, resp2]
    assert sorted(obj["id"] for obj in client.get("")) == list(range(18))
