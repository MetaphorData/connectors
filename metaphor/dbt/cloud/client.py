import json
import shutil
import tempfile
from os import path
from typing import Dict, Optional, Tuple

import requests

from metaphor.common.logger import get_logger

logger = get_logger()


class DbtAdminAPIClient:
    """A client that wraps the dbt Cloud Administrative API

    See https://docs.getdbt.com/dbt-cloud/api-v2 for more details.
    """

    def __init__(self, base_url: str, account_id: int, service_token: str):
        self.admin_api_base_url = f"{base_url}/api/v2"
        self.account_id = account_id
        self.service_token = service_token

    def _get(self, path: str, params: Optional[Dict] = None):
        url = f"{self.admin_api_base_url}/accounts/{self.account_id}/{path}"
        logger.debug(f"Sending request to {url}")
        req = requests.get(
            url,
            params=params,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Token {self.service_token}",
            },
            timeout=600,  # request timeout 600s
        )

        assert req.status_code == 200, f"{url} returned {req.status_code}"
        return req.json()

    def get_last_successful_run(self, job_id: int) -> Tuple[int, int]:
        """Get the project and run IDs of the last successful run for a job"""

        offset = 0
        page_size = 50
        while True:
            # https://docs.getdbt.com/dbt-cloud/api-v2#operation/listRunsForAccount
            resp = self._get(
                "runs/",
                {
                    "job_definition_id": job_id,
                    "order_by": "-id",
                    "limit": page_size,
                    "offset": offset,
                },
            )

            data = resp.get("data")
            assert len(data) > 0, "Unable to find any successful run"

            for run in data:
                if run.get("status") == 10:
                    return run.get("project_id"), run.get("id")

            offset += page_size

    def get_snowflake_account(self, project_id: int) -> Optional[str]:
        """Get the Snowflake account name for a particular project

        Return None if the project doesn't have a Snowflake connection
        """

        # https://docs.getdbt.com/dbt-cloud/api-v2#operation/getProjectById
        resp = self._get(f"projects/{project_id}")
        connection = resp.get("data").get("connection", {})
        if connection.get("type") != "snowflake":
            return None

        return connection.get("details").get("account")

    def get_run_artifact(self, job_id: int, run_id: int, artifact: str) -> str:
        """Write a particular artifact from a run to a temp file."""

        # https://docs.getdbt.com/dbt-cloud/api-v2#operation/getArtifactsByRunId
        artifact_json = self._get(f"runs/{run_id}/artifacts/{artifact}")

        fd, temp_name = tempfile.mkstemp()
        with open(fd, "w") as fp:
            json.dump(artifact_json, fp)

        pretty_name = f"{path.dirname(temp_name)}/{job_id}-{artifact}"
        shutil.copyfile(temp_name, pretty_name)
        return pretty_name
