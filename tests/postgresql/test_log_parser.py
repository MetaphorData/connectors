from metaphor.postgresql.log_parser import parse_postgres_log


def test_parse_postgres_log():
    assert parse_postgres_log("2024-08-29 09:25:50 UTC:foo@metaphor:[615]:LOG") is None
