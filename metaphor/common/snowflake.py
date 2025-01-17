PRIVATE_LINK_SUFFIX = ".privatelink"
SNOWFLAKE_HOST_SUFFIX = ".snowflakecomputing.com"
SNOWFLAKE_DEFAULT_REGION = ".us-west-2"


def normalize_snowflake_account(host: str, remove_default_region: bool = False) -> str:
    """
    Normalize different variations of Snowflake account.
    See https://docs.snowflake.com/en/user-guide/admin-account-identifier
    """

    # Account name is case insensitive
    host = host.lower()

    if host.endswith(SNOWFLAKE_HOST_SUFFIX):
        host = host[: -len(SNOWFLAKE_HOST_SUFFIX)]

    # Strip PrivateLink suffix, e.g. account.privatelink.snowflakecomputing.com
    if host.endswith(PRIVATE_LINK_SUFFIX):
        host = host[: -len(PRIVATE_LINK_SUFFIX)]

    # Remove default region (us-west-2) if applicable, e.g. account.us-west-2.snowflakecomputing.com
    # This is to keep the account name consistent with the results from the snowflake crawler
    if remove_default_region and host.endswith(SNOWFLAKE_DEFAULT_REGION):
        host = host[: -len(SNOWFLAKE_DEFAULT_REGION)]

    return host
