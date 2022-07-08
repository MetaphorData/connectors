import pytest
from pydantic import ValidationError

from metaphor.common.git import GitRepoConfig, _generate_git_url


def test_sampling_config():
    assert (
        _generate_git_url(
            GitRepoConfig(
                git_url="https://github.com/foo/looker.git",
                username="foo",
                access_token="bar",
            )
        )
        == "https://foo:bar@github.com/foo/looker.git"
    )

    assert (
        _generate_git_url(
            GitRepoConfig(
                git_url="https://gitlab.com/abc.git",
                username="oauth2",
                access_token="bar",
            )
        )
        == "https://oauth2:bar@gitlab.com/abc.git"
    )

    assert (
        _generate_git_url(
            GitRepoConfig(
                git_url="https://gitlab.com/abc.git",
                username="foo",
                password="bar",
            )
        )
        == "https://foo:bar@gitlab.com/abc.git"
    )

    assert (
        _generate_git_url(
            GitRepoConfig(
                git_url="https://abc@bitbucket.org:80/a/test.git",
                username="foo",
                password="bar",
            )
        )
        == "https://foo:bar@bitbucket.org:80/a/test.git"
    )

    with pytest.raises(ValidationError):
        GitRepoConfig(
            git_url="https://github.com/foo/looker.git",
            username="foo",
        )
