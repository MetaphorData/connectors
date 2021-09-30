import json
import logging
from dataclasses import dataclass
from typing import List

from requests import HTTPError, post
from serde import deserialize

from .sink import Sink

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@deserialize
@dataclass
class ApiSinkConfig:
    url: str
    api_key: str
    batch_size: int = 20
    timeout: int = 30  # default 30 seconds timeout


class ApiSink(Sink):
    """Ingestion API sink functions"""

    def __init__(self, config: ApiSinkConfig):
        self._url = config.url
        self._post_header = {"x-api-key": config.api_key}
        self._batch_size = config.batch_size
        self._timeout = config.timeout

    def _sink(self, messages: List[dict]) -> bool:
        """Post messages to Ingestion API"""
        no_error = True
        for chunk in self._chunks(messages, self._batch_size):
            response = None
            try:
                response = post(
                    self._url,
                    json=chunk,
                    headers=self._post_header,
                    timeout=self._timeout,
                )

                # If the response was successful, no Exception will be raised
                response.raise_for_status()
                logger.info(f"Posted {len(chunk)} messages")
                logger.debug(f"POST response {json.dumps(response.text)}")
            except HTTPError as http_error:
                logger.error(
                    f"HTTP error {http_error}, response {json.dumps(response.text) if response is not None else ''}"
                )
                no_error = False
            except Exception as error:
                logger.error(
                    f"POST error: {error}, response {json.dumps(response.text) if response is not None else ''}"
                )
                no_error = False

        logger.info(f"Posted total {len(messages)} messages")
        return no_error
