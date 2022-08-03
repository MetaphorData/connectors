from metaphor.common.cli import cli_main
from metaphor.snowflake.extractor import SnowflakeExtractor


def main(config_file: str):
    cli_main(SnowflakeExtractor, config_file)
