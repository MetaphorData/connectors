import os
import tempfile
from typing import Optional
from urllib.parse import urlparse

from git import Repo
from pydantic import root_validator
from pydantic.dataclasses import dataclass


@dataclass
class GitRepoConfig:

    # Git repository URL, ending with ".git"
    git_url: str

    # git repo access token
    access_token: Optional[str] = None

    # git repo username
    username: Optional[str] = None

    # git repo password
    password: Optional[str] = None

    # path to the project, default to the root of the repo
    project_path: str = "."

    @root_validator()
    def have_token_or_username_password(cls, values):
        if values["access_token"] is None and (
            values["username"] is None or values["password"] is None
        ):
            raise ValueError("must set either access_token or username+password")
        return values


def clone_repo(config: GitRepoConfig, local_dir: Optional[str] = None) -> str:
    """
    Clone a git repo to local storage and return the path to the project
    If local directory not provided, use generated temp directory
    """
    if local_dir is None:
        local_dir = tempfile.mkdtemp()

    parsed_url = urlparse(config.git_url)
    auth = config.access_token or f"{config.username}:{config.password}"
    git_url = f"{parsed_url.scheme}://{auth}@{parsed_url.netloc}{parsed_url.path}"

    Repo.clone_from(git_url, local_dir)
    return os.path.join(local_dir, config.project_path)
