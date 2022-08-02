from metaphor.common.cli import cli_main
from metaphor.snowflake.usage.extractor import SnowflakeUsageExtractor


def main(config_file: str):
    cli_main(SnowflakeUsageExtractor, config_file)
