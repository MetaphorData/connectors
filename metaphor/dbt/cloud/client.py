from typing import Any, Dict, List, Optional

import requests
from pydantic import BaseModel

from metaphor.common.logger import get_logger

logger = get_logger()


class DbtConnection(BaseModel, extra="allow"):
    account_id: int
    project_id: Optional[int] = None
    name: str
    type: str
    state: int
    details: Dict[str, Any]


class DbtProject(BaseModel, extra="allow"):
    id: int
    name: str
    account_id: int
    description: Optional[str] = None
    connection_id: int
    created_at: str
    updated_at: str
    deleted_at: Optional[str] = None
    connection: DbtConnection


class DbtEnvironment(BaseModel, extra="allow"):
    id: int
    account_id: int
    project_id: int
    name: str
    dbt_version: str
    type: str
    deployment_type: Optional[str] = None
    state: int
    created_at: str
    updated_at: str


class DbtAdminAPIClient:
    """A client that wraps the dbt Cloud Administrative API

    See https://docs.getdbt.com/dbt-cloud/api-v3 for more details.
    """

    def __init__(
        self,
        base_url: str,
        account_id: int,
        service_token: str,
    ):
        self.admin_api_base_url = f"{base_url}/api/v3"
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
            timeout=60,  # request timeout 60s
        )

        assert req.status_code == 200, f"{url} returned {req.status_code}"
        return req.json()

    def list_projects(self) -> List[DbtProject]:
        """Get all projects in the account"""
        resp = self._get("projects/")
        return [DbtProject.model_validate(project) for project in resp.get("data")]

    def list_environments(self, project_id: int) -> List[DbtEnvironment]:
        """Get all environments under the project"""
        resp = self._get(f"projects/{project_id}/environments/")
        return [DbtEnvironment.model_validate(env) for env in resp.get("data")]
