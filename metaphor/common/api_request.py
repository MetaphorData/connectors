import json
from typing import Any, Callable, Dict, Type, TypeVar

import requests
from pydantic import parse_obj_as

from metaphor.common.logger import get_logger

logger = get_logger()
T = TypeVar("T")


class ApiError(Exception):
    def __init__(self, url: str, status_code: int, error_msg: str) -> None:
        self.status_code = status_code
        self.error_msg = error_msg
        super().__init__(f"call {url} api failed: {status_code}\n{error_msg}")


def get_request(
    url: str,
    headers: Dict[str, str],
    type_: Type[T],
    transform_response: Callable[[requests.Response], Any] = lambda r: r.json(),
) -> T:
    """Generic get api request to make third part api call and return with customized data class"""

    result = requests.get(url, headers=headers)
    if result.status_code == 200:
        logger.debug(json.dumps(result.json(), indent=2))
        return parse_obj_as(type_, transform_response(result))
    else:
        raise ApiError(url, result.status_code, result.content.decode())
