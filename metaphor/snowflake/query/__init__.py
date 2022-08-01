from metaphor.common.cli import cli_main
from metaphor.snowflake.query.extractor import SnowflakeQueryExtractor


def main(config_file: str):
    cli_main(SnowflakeQueryExtractor, config_file)
