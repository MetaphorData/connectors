from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from pydantic import BaseModel, Field

from metaphor.common.entity_id import dataset_normalized_name


class DiscoveryTestNode(BaseModel):
    uniqueId: str
    name: Optional[str]
    status: Optional[str]
    columnName: Optional[str]
    executeCompletedAt: Optional[datetime]
    dependsOn: List[str]

    @property
    def models(self) -> List[str]:
        return [x for x in self.dependsOn if x.startswith("model.")]


class DiscoveryModelNode(BaseModel):
    uniqueId: str
    name: Optional[str]
    alias: Optional[str]
    database: Optional[str]
    schema_: Optional[str] = Field(alias="schema")

    @property
    def normalized_name(self) -> str:
        return dataset_normalized_name(self.database, self.schema_, self.name)


class DiscoveryAPI:
    """
    A wrapper around dbt cloud's discovery API.
    """

    def __init__(self, url: str, token: str) -> None:
        self.url = url
        self.token = token

    def _send(self, query: str, variables: Dict[str, Any]):
        resp = requests.post(
            url=self.url,
            headers={
                "authorization": f"Bearer {self.token}",
                "content-type": "application/json",
            },
            json={"query": query, "variables": variables},
            timeout=15,
        )
        return resp.json()["data"]

    def get_all_job_model_names(self, job_id: int):
        query = """
query Models($jobId: BigInt!) {
    job(id: $jobId) {
        models {
            alias
            database
            name
            schema
            uniqueId
        }
    }
}
        """
        variables = {
            "jobId": job_id,
        }

        model_nodes = self._send(query, variables)["job"]["models"]
        res: Dict[str, str] = {}
        for model_node in model_nodes:
            model = DiscoveryModelNode.model_validate(model_node)
            res[model.uniqueId] = model.normalized_name
        return res

    def get_all_job_tests(self, job_id: int):
        query = """
query Tests($jobId: BigInt!) {
  job(id: $jobId) {
    tests {
        uniqueId
        name
        status
        columnName
        executeCompletedAt
        dependsOn
    }
  }
}
        """
        variables = {
            "jobId": job_id,
        }
        tests = self._send(query, variables)["job"]["tests"]
        return [DiscoveryTestNode.model_validate(test) for test in tests]
