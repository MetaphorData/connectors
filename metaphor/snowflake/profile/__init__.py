from metaphor.common.cli import cli_main
from metaphor.snowflake.profile.extractor import SnowflakeProfileExtractor


def main(config_file: str):
    cli_main(SnowflakeProfileExtractor, config_file)
