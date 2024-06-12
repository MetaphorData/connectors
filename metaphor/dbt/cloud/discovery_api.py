from typing import Any, Dict

import requests

from metaphor.common.entity_id import dataset_normalized_name


class DiscoveryAPI:
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

    def get_model_dataset_name(self, job_id: int, model_unique_id: str):
        query = """
query Model($uniqueId: String!, $jobId: BigInt!) {
    job(id: $jobId) {
        model(uniqueId: $uniqueId) {
            alias
            database
            name
            schema
        }
    }
}
        """
        variables = {
            "uniqueId": model_unique_id,
            "jobId": job_id,
        }

        model = self._send(query, variables)["job"]["model"]
        database, schema, name = (
            model.get("database"),
            model.get("schema"),
            model.get("alias") or model.get("name"),
        )
        return dataset_normalized_name(database, schema, name)

    def get_test_status(self, job_id: int, test_unique_id: str):
        query = """
query Test($uniqueId: String!, $jobId: BigInt!) {
  job(id: $jobId) {
    test(uniqueId: $uniqueId) {
      status
    }
  }
}
        """
        variables = {
            "jobId": job_id,
            "uniqueId": test_unique_id,
        }

        res = self._send(query, variables)["job"]["test"]
        if res is None:
            return "skipped"
        return res["status"]
