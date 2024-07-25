import os
from typing import Any, Dict

from ..targets import job_targets

dir_path = os.path.dirname(os.path.realpath(__file__))


def fake_GetJobRunMacros(variables: Dict[str, Any]):
    target = job_targets[variables["jobId"]]
    with open(f"{dir_path}/{target}.json") as f:
        return f.read()
