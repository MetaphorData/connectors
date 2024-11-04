from typing import Dict
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import BaseModel

from metaphor.common.api_request import ApiError, make_request


class DummyResult(BaseModel):
    foo: str


@patch("requests.get")
def test_get_request_200(mock_get: MagicMock):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"foo": "bar"}
    mock_get.return_value = mock_response

    result = make_request("http://test.com", {}, DummyResult)
    assert result.foo == "bar"


@patch("requests.get")
def test_get_request_not_200(mock_get: MagicMock):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    try:
        make_request("http://test.com", {}, Dict)
        assert False, "ApiError not thrown"
    except ApiError:
        assert True


def test_requests_timeout():
    # Simple demo webserver to test delayed responses
    # https://httpbin.org/#/Dynamic_data/get_delay__delay_
    url = "https://httpbin.org/delay/2"

    # This times out
    with pytest.raises(requests.Timeout):
        make_request(url, headers={"accept": "application/json"}, type_=Dict, timeout=1)

    # This is OK
    resp = make_request(
        url, headers={"accept": "application/json"}, type_=Dict, timeout=10
    )
    assert resp
