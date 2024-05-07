from metaphor.snowflake.utils import to_quoted_identifier


def test_to_quoted_identifier():
    assert to_quoted_identifier([None, "", "a", "b", "c"]) == '"a"."b"."c"'

    assert to_quoted_identifier(["db", "sc", 'ta"@BLE']) == '"db"."sc"."ta""@BLE"'
