from metaphor.common.cli import cli_main
from metaphor.redshift.lineage.extractor import RedshiftLineageExtractor


def main(config_file: str):
    cli_main(RedshiftLineageExtractor, config_file)
