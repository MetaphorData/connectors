PRIVATE_LINK_SUFFIX = ".privatelink"


def normalize_snowflake_account(account: str) -> str:
    """
    Normalize different variations of Snowflake account.
    See https://docs.snowflake.com/en/user-guide/admin-account-identifier
    """

    # Account name is case insensitive
    account = account.lower()

    # Strip PrivateLink suffix
    if account.endswith(PRIVATE_LINK_SUFFIX):
        return account[: -len(PRIVATE_LINK_SUFFIX)]

    return account
