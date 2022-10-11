import json
from typing import Any, Callable, Dict, Type, TypeVar

import requests
from pydantic import parse_obj_as

from metaphor.common.exception import AuthenticationError, EntityNotFoundError
from metaphor.common.logger import get_logger

logger = get_logger(__name__)
T = TypeVar("T")


def call_get(
    url: str,
    headers: Dict[str, str],
    type_: Type[T],
    transform_response: Callable[[requests.Response], Any] = lambda r: r.json(),
) -> T:
    result = requests.get(url, headers=headers)
    body = result.content.decode()

    if result.status_code == 401:
        raise AuthenticationError(body)
    elif result.status_code == 404:
        raise EntityNotFoundError(body)
    elif result.status_code != 200:
        raise AssertionError(f"GET {url} failed: {result.status_code}\n{body}")

    logger.debug(f"Response from {url}:")
    logger.debug(json.dumps(result.json(), indent=2))
    return parse_obj_as(type_, transform_response(result))
