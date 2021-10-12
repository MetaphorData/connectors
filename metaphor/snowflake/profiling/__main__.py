from metaphor.common.cli import cli_main

from .extractor import SnowflakeProfilingExtractor, SnowflakeProfilingRunConfig

if __name__ == "__main__":
    cli_main(
        "Snowflake column profiling metadata extractor",
        SnowflakeProfilingRunConfig,
        SnowflakeProfilingExtractor,
    )
