from typing import Dict
from unittest.mock import MagicMock, patch

from pydantic import BaseModel

from metaphor.common.api_request import ApiError, get_request


class DummyResult(BaseModel):
    foo: str


def test_get_request_200():
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"foo": "bar"}
        mock_get.return_value = mock_response

        result = get_request("http://test.com", {}, DummyResult)
        assert result.foo == "bar"


def test_get_request_not_200():
    with patch("requests.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        try:
            get_request("http://test.com", {}, Dict)
            assert False, "ApiError not thrown"
        except ApiError:
            assert True
