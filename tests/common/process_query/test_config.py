from metaphor.common.process_query import ProcessQueryConfig
from metaphor.common.process_query.config import RedactLiteralConfig


def test_config():
    config = ProcessQueryConfig(redact=RedactLiteralConfig(where_clauses=True))
    assert config.should_process

    config = ProcessQueryConfig(
        redact=RedactLiteralConfig(when_not_matched_insert_clauses=True)
    )
    assert config.should_process

    config = ProcessQueryConfig()
    assert config.should_process
