from metaphor.common.cli import cli_main
from metaphor.snowflake.lineage.extractor import SnowflakeLineageExtractor


def main(config_file: str):
    cli_main(SnowflakeLineageExtractor, config_file)
