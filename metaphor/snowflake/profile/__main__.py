from metaphor.common.cli import cli_main

from .extractor import SnowflakeProfileExtractor, SnowflakeProfileRunConfig

if __name__ == "__main__":
    cli_main(
        "Snowflake data profile extractor",
        SnowflakeProfileRunConfig,
        SnowflakeProfileExtractor,
    )
