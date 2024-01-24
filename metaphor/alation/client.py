from typing import Any, Dict, Iterator, Optional

import requests


class Client:
    def __init__(self, base_url: str, headers: Dict[str, str]) -> None:
        self.base_url = base_url
        self.headers = headers

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Iterator[Any]:
        url: Optional[str] = f"{self.base_url}/{path}"
        while url:
            response = requests.get(
                url,
                params=params if params else None,
                headers=self.headers,
                timeout=10,
            )
            response.raise_for_status()
            url = response.headers.get("X-Next-Page")
            # `response.json()` is a list of objects, we're returning them as an iterator
            for obj in response.json():
                yield obj
