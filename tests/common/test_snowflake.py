from metaphor.common.snowflake import normalize_snowflake_account


def test_normalize_snowflake_account():
    assert normalize_snowflake_account("org-acc") == "org-acc"
    assert normalize_snowflake_account("org.us-west-1") == "org.us-west-1"
    assert normalize_snowflake_account("foo.privatelink") == "foo"
