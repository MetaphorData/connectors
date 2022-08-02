from metaphor.common.cli import cli_main
from metaphor.redshift.query.extractor import RedshiftQueryExtractor


def main(config_file: str):
    cli_main(RedshiftQueryExtractor, config_file)
