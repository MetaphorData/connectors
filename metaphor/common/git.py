import os
import tempfile
from typing import Optional
from urllib.parse import quote, urlparse

from git import Repo
from pydantic import root_validator
from pydantic.dataclasses import dataclass

from metaphor.common.dataclass import ConnectorConfig
from metaphor.common.utils import must_set_exactly_one


@dataclass(config=ConnectorConfig)
class GitRepoConfig:
    # Git repository URL, ending with ".git"
    git_url: str

    # git repo username
    username: str

    # git repo access token
    access_token: Optional[str] = None

    # git repo password
    password: Optional[str] = None

    # relative path to the project, default to the root of the repo
    project_path: str = ""

    @root_validator
    def have_token_or_password(cls, values):
        must_set_exactly_one(values, ["access_token", "password"])
        return values


def clone_repo(config: GitRepoConfig, local_dir: Optional[str] = None) -> str:
    """
    Clone a git repo to local storage and return the path to the project
    If local directory not provided, use generated temp directory
    """
    if local_dir is None:
        local_dir = tempfile.mkdtemp()

    git_url = _generate_git_url(config)

    Repo.clone_from(git_url, local_dir)
    return os.path.join(local_dir, config.project_path)


def _generate_git_url(config: GitRepoConfig) -> str:
    """Generate a git URL containing authentication"""
    parsed_url = urlparse(config.git_url)

    auth = f"{quote(config.username)}:{config.access_token or config.password}"
    host = (parsed_url.hostname or parsed_url.netloc) + (
        f":{parsed_url.port}" if parsed_url.port else ""
    )
    return f"{parsed_url.scheme}://{auth}@{host}{parsed_url.path}"
