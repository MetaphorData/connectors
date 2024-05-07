from metaphor.snowflake.utils import fix_truncated_query_text, to_quoted_identifier


def test_to_quoted_identifier():
    assert to_quoted_identifier([None, "", "a", "b", "c"]) == '"a"."b"."c"'

    assert to_quoted_identifier(["db", "sc", 'ta"@BLE']) == '"db"."sc"."ta""@BLE"'


def test_fix_truncated_query_text():
    truncated = fix_truncated_query_text(
        'INSERT INTO tb (col1, col2, col3, col4) VALUES ("Hey, I just met you,", "And this is crazy,", "But here\'s my number,", "So call me, maybe',  # Some joke I saw some time ago: https://stackoverflow.com/questions/2139812/what-is-a-callback
    )

    assert (
        truncated
        == "INSERT INTO tb (col1, col2, col3, col4) VALUES (NULL, NULL, NULL, NULL)"
    )
