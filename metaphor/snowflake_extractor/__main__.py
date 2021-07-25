from metaphor.common.cli import cli_main

from .extractor import SnowflakeExtractor, SnowflakeRunConfig

if __name__ == "__main__":
    cli_main("Snowflake metadata extractor", SnowflakeRunConfig, SnowflakeExtractor)
