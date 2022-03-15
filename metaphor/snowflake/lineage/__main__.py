from metaphor.common.cli import cli_main

from .extractor import SnowflakeLineageExtractor

if __name__ == "__main__":
    cli_main("Snowflake lineage extractor", SnowflakeLineageExtractor)
