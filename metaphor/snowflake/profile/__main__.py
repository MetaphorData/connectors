from metaphor.common.cli import cli_main

from .extractor import SnowflakeProfileExtractor

if __name__ == "__main__":
    cli_main("Snowflake data profile extractor", SnowflakeProfileExtractor)
