from metaphor.common.cli import cli_main

from .extractor import SnowflakeUsageExtractor, SnowflakeUsageRunConfig

if __name__ == "__main__":
    cli_main(
        "Snowflake usage metadata extractor",
        SnowflakeUsageRunConfig,
        SnowflakeUsageExtractor,
    )
