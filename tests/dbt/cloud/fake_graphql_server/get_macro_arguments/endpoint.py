import os
from typing import Any, Dict

from ..targets import environment_targets

dir_path = os.path.dirname(os.path.realpath(__file__))


def fake_GetMacroArguments(variables: Dict[str, Any]):
    target = environment_targets[variables["environmentId"]]
    with open(f"{dir_path}/{target}.json") as f:
        return f.read()
