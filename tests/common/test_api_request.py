from typing import Dict
from unittest.mock import MagicMock, patch

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
    mock_get.__name__ = "get"

    result = make_request("http://test.com", {}, DummyResult)
    assert result.foo == "bar"


@patch("requests.get")
def test_get_request_not_200(mock_get: MagicMock):
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response
    mock_get.__name__ = "get"

    try:
        make_request("http://test.com", {}, Dict)
        assert False, "ApiError not thrown"
    except ApiError:
        assert True
