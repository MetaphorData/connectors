import os
from typing import Any, Dict

dir_path = os.path.dirname(os.path.realpath(__file__))


def fake_GetJobRunSources(variables: Dict[str, Any]):
    with open(f"{dir_path}/jaffle_shop.json") as f:
        return f.read()
