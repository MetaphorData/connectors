from metaphor.common.sql.process_query import ProcessQueryConfig
from metaphor.common.sql.process_query.config import RedactPIILiteralsConfig


def test_config():
    config = ProcessQueryConfig(ignore_insert_values_into=True)
    assert config.should_process

    config = ProcessQueryConfig(redact_literals=RedactPIILiteralsConfig(enabled=True))
    assert config.should_process

    config = ProcessQueryConfig()
    assert not config.should_process

    config = ProcessQueryConfig(ignore_command_statement=True)
    assert config.should_process

    config = ProcessQueryConfig(
        redact_literals=RedactPIILiteralsConfig(where_clauses=True),
        ignore_command_statement=False,
    )
    assert not config.should_process
