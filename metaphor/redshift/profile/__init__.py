from metaphor.common.cli import cli_main
from metaphor.redshift.profile.extractor import RedshiftProfileExtractor


def main(config_file: str):
    cli_main(RedshiftProfileExtractor, config_file)
