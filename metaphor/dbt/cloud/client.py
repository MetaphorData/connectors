import json
import shutil
import tempfile
from os import path
from typing import Dict, List, NamedTuple, Optional, Set

import requests

from metaphor.common.logger import get_logger

logger = get_logger()


class DbtRun(NamedTuple):
    project_id: int
    job_id: int
    run_id: int

    def __str__(self) -> str:
        return f"ID = {self.run_id}, project ID = {self.project_id}, job ID = {self.job_id}"


class DbtAdminAPIClient:
    """A client that wraps the dbt Cloud Administrative API

    See https://docs.getdbt.com/dbt-cloud/api-v2 for more details.
    """

    def __init__(
        self,
        base_url: str,
        account_id: int,
        service_token: str,
        included_env_ids: Set[int] = set(),
    ):
        self.admin_api_base_url = f"{base_url}/api/v2"
        self.account_id = account_id
        self.service_token = service_token
        self.included_env_ids = included_env_ids

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

    def get_project_jobs(self, project_id: int) -> List[int]:
        offset = 0
        page_size = 50
        payload = {
            "limit": page_size,
            "offset": offset,
            "project_id": project_id,
        }
        jobs: Set[int] = set()
        while True:
            # https://docs.getdbt.com/dbt-cloud/api-v2#/operations/List%20Jobs
            resp = self._get("jobs/", payload)
            data = resp.get("data")

            # FIXME dbt pagination is buggy, if we go to a page that isn't supposed to have
            # things it'll just return the last page that has stuff inside
            new_jobs = {job["id"] for job in data}
            # If everything in the current page have already been found, then we're done
            if not any(job for job in new_jobs if job not in jobs):
                return list(jobs)

            jobs |= new_jobs
            offset += page_size
            payload["offset"] = offset

    def is_job_included(self, job_id: int) -> bool:
        if len(self.included_env_ids) == 0:
            # No excluded environment, just return True
            return True

        resp = self._get(f"jobs/{job_id}")
        data = resp.get("data")
        return int(data.get("environment_id", -1)) in self.included_env_ids

    def get_last_successful_run(
        self, job_id: int, page_size: int = 50
    ) -> Optional[DbtRun]:
        """Get the run ID of the last successful run for a job"""

        offset = 0
        payload = {
            "order_by": "-id",
            "limit": page_size,
            "offset": offset,
            "job_definition_id": job_id,
        }

        while True:
            # https://docs.getdbt.com/dbt-cloud/api-v2#operation/listRunsForAccount
            resp = self._get("runs/", payload)

            data = resp.get("data")
            if len(data) == 0:
                return None

            for run in data:
                if run.get("status") == 10:
                    return DbtRun(
                        project_id=run.get("project_id"),
                        job_id=run.get("job_definition_id"),
                        run_id=run.get("id"),
                    )

            offset += page_size
            payload["offset"] = offset

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

    def get_run_artifact(self, run: DbtRun, artifact: str) -> str:
        """Write a particular artifact from a run to a temp file."""

        # https://docs.getdbt.com/dbt-cloud/api-v2#operation/getArtifactsByRunId
        artifact_json = self._get(f"runs/{run.run_id}/artifacts/{artifact}")

        fd, temp_name = tempfile.mkstemp()
        with open(fd, "w") as fp:
            json.dump(artifact_json, fp)

        pretty_name = (
            f"{path.dirname(temp_name)}/{run.project_id}-{run.job_id}-{artifact}"
        )
        shutil.copyfile(temp_name, pretty_name)
        return pretty_name
