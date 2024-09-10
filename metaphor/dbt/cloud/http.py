import json
import secrets
from collections.abc import Iterable

import httpx

from metaphor.common.logger import get_logger, json_dump_to_debug_file

logger = get_logger()


class LogResponse(httpx.Response):
    def _log_payload(self, filename: str, payload: bytes):
        try:
            request_json = json.loads(payload.decode())
            json_dump_to_debug_file(request_json, filename)
        except json.JSONDecodeError:
            logger.exception("Not able to log request")

    def _log(self, response: bytes):
        request = self.request
        if not isinstance(request.stream, Iterable):
            return

        r_body = b"".join(request.stream)
        method = request.method

        request_signature = f"{method}_{request.url.path[1:].replace('/', u'__')}"
        random_slug = secrets.token_hex(4)
        req_suffix = f"_{random_slug}_req.json"
        res_suffix = f"_{random_slug}_res.json"
        req_file_name = f"{request_signature[:250 - len(req_suffix)]}{req_suffix}"
        res_file_name = f"{request_signature[:250 - len(res_suffix)]}{res_suffix}"

        self._log_payload(req_file_name, r_body)
        self._log_payload(res_file_name, response)

    def read(self) -> bytes:
        resp = super().read()
        self._log(resp)
        return resp


class LogTransport(httpx.BaseTransport):
    def __init__(self, transport: httpx.BaseTransport):
        self.transport = transport

    def handle_request(self, request: httpx.Request) -> httpx.Response:

        response = self.transport.handle_request(request)

        return LogResponse(
            status_code=response.status_code,
            headers=response.headers,
            stream=response.stream,
            extensions=response.extensions,
        )
