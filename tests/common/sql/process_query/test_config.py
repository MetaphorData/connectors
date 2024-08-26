from metaphor.common.sql.process_query import ProcessQueryConfig
from metaphor.common.sql.process_query.config import RedactPIILiteralsConfig


def test_config():
    config = ProcessQueryConfig(ignore_insert_values_into=True)
    assert config.should_process

    config = ProcessQueryConfig(redact_literals=RedactPIILiteralsConfig(redact=True))
    assert config.should_process

    config = ProcessQueryConfig()
    assert not config.should_process

    config = ProcessQueryConfig(
        redact_literals=RedactPIILiteralsConfig(where_clauses=True)
    )
    assert not config.should_process
