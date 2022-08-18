import json
from unittest.mock import MagicMock


def load_json(path):
    with open(path, "r") as f:
        return json.load(f)


def compare_list_ignore_order(a: list, b: list):
    t = list(b)  # make a mutable copy
    try:
        for elem in a:
            t.remove(elem)
    except ValueError:
        return False
    return not t


# Backport AsyncMock support to Python 3.7
# https://stackoverflow.com/a/32498408
class AsyncMock(MagicMock):
    async def __call__(self, *args, **kwargs):
        return super(AsyncMock, self).__call__(*args, **kwargs)
