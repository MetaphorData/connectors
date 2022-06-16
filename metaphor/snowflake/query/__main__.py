from metaphor.common.cli import cli_main

from .extractor import SnowflakeQueryExtractor

if __name__ == "__main__":
    cli_main("Snowflake query history extractor", SnowflakeQueryExtractor)
