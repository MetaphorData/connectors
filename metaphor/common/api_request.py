import json
import secrets
import tempfile
from typing import Any, Callable, Dict, Literal, Type, TypeVar
from urllib.parse import urljoin, urlparse

import requests
from pydantic import TypeAdapter, ValidationError

from metaphor.common.logger import debug_files, get_logger

logger = get_logger()
T = TypeVar("T")


class ApiError(Exception):
    def __init__(self, url: str, status_code: int, body: str) -> None:
        self.status_code = status_code
        self.body = body
        super().__init__(f"call {url} api failed: {status_code}\n{body}")


def make_request(
    url: str,
    headers: Dict[str, str],
    type_: Type[T],
    transform_response: Callable[[requests.Response], Any] = lambda r: r.json(),
    timeout: int = 10,
    method: Literal["get", "post"] = "get",
    **kwargs,
) -> T:
    """Generic get api request to make third part api call and return with customized data class"""
    result = getattr(requests, method)(url, headers=headers, timeout=timeout, **kwargs)
    if result.status_code == 200:

        # request signature, example: get_v1__resource_abcd
        request_signature = f"{method}_{urlparse(url).path[1:].replace('/', u'__')}"

        # suffix with length 8 chars random string
        suffix = f"_{secrets.token_hex(4)}.json"

        # Avoid file name too long error and truncate prefix to avoid duplicate file name
        # 250 is the lowest default maximum characters file name length limit across major file systems
        file_name = f"{request_signature[:250 - len(suffix)]}{suffix}"

        # Add JSON response to log.zip
        out_file = f"{tempfile.mkdtemp()}/{file_name}"
        with open(out_file, "w") as fp:
            json.dump(result.json(), fp, indent=2)
        debug_files.append(out_file)

        try:
            return TypeAdapter(type_).validate_python(transform_response(result))
        except ValidationError as error:
            logger.error(
                f"url: {url}, result: {json.dumps(result.json())}, error: {error}"
            )
            raise ApiError(url, result.status_code, "cannot parse result")
    else:
        raise ApiError(url, result.status_code, result.content.decode())


def make_url(base: str, path: str):
    return urljoin(base, path)
