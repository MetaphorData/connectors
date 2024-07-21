from metaphor.common.sql.process_query import ProcessQueryConfig
from metaphor.common.sql.process_query.config import RedactPIILiteralsConfig


def test_config():
    config = ProcessQueryConfig(
        redact_literals=RedactPIILiteralsConfig(where_clauses=True)
    )
    assert config.should_process

    config = ProcessQueryConfig(
        redact_literals=RedactPIILiteralsConfig(when_not_matched_insert_clauses=True)
    )
    assert config.should_process

    config = ProcessQueryConfig()
    assert not config.should_process
