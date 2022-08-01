from metaphor.common.cli import cli_main
from metaphor.redshift.usage.extractor import RedshiftUsageExtractor


def main(config_file: str):
    cli_main(RedshiftUsageExtractor, config_file)
