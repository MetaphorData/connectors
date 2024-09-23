PRIVATE_LINK_SUFFIX = ".privatelink"
SNOWFLAKE_HOST_SUFFIX = ".snowflakecomputing.com"


def normalize_snowflake_account(host: str) -> str:
    """
    Normalize different variations of Snowflake account.
    See https://docs.snowflake.com/en/user-guide/admin-account-identifier
    """

    # Account name is case insensitive
    host = host.lower()

    if host.endswith(SNOWFLAKE_HOST_SUFFIX):
        host = host[: -len(SNOWFLAKE_HOST_SUFFIX)]

    # Strip PrivateLink suffix
    if host.endswith(PRIVATE_LINK_SUFFIX):
        return host[: -len(PRIVATE_LINK_SUFFIX)]

    return host
