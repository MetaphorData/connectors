from metaphor.bigquery.lineage.extractor import BigQueryLineageExtractor
from metaphor.common.cli import cli_main


def main(config_file: str):
    cli_main(BigQueryLineageExtractor, config_file)
