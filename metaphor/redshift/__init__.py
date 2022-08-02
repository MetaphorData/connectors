from metaphor.common.cli import cli_main
from metaphor.redshift.extractor import RedshiftExtractor


def main(config_file: str):
    cli_main(RedshiftExtractor, config_file)
