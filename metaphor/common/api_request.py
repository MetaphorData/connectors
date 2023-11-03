import json
import secrets
import tempfile
from typing import Any, Callable, Dict, Type, TypeVar
from urllib.parse import urlparse

import requests
from pydantic import TypeAdapter, ValidationError

from metaphor.common.logger import debug_files, get_logger

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
    timeout: int = 600,  # default request timeout 600s
    **kwargs,
) -> T:
    """Generic get api request to make third part api call and return with customized data class"""
    result = requests.get(url, headers=headers, timeout=timeout, **kwargs)
    if result.status_code == 200:
        # Add JSON response to log.zip
        file_name = (
            f"{urlparse(url).path[1:].replace('/', u'__')}_{secrets.token_hex(4)}"
        )
        # Avoid file name too long error and truncate prefix to avoid duplicate file name
        # 250 is the lowest default maximum charactors file name length limit acrocess major file systems
        file_name = (
            file_name[len(file_name) - 245 :] if len(file_name) > 245 else file_name
        )
        file_name = f"{file_name}.json"
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
