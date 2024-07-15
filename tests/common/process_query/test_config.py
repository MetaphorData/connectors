from metaphor.common.process_query import ProcessQueryConfig


def test_config():
    config = ProcessQueryConfig(redact_literal_values_in_where_clauses=True)
    assert config.should_process
