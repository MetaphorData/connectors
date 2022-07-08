# Git Repo Connection Config

When the connector needs to fetch source code from a git repository, we can set up the connection config by using personal access token or the username and password. 

## Personal Access Token

Using personal access token with read scope is the recommended way to connect to git repository. To create a new token, please refer to the instructions for [Github](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token) or [GitLab](https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html#create-a-personal-access-token). The configuration will be:

```yaml
git_url: <git_repo_url> # ending with .git
username: <username>
access_token: <personal_access_token>
```

> NOTE: For GitLab or GitLab enterprise, please use `oauth2` as the username:

## Username Password

Alternatively one can choose to use password to authenticate with the git service. The configuration will be:

```yaml
git_url: <git_repo_url> # ending with .git
username: <username>
password: <password>
```

> NOTE: BitBucket Cloud [stopped supporting account passwords for Git authentication](https://atlassian.community/t5/x/x/ba-p/1948231). Please generate and use [App passwords](https://support.atlassian.com/bitbucket-cloud/docs/app-passwords/) instead.
