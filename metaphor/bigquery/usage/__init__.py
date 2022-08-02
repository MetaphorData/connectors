from metaphor.bigquery.usage.extractor import BigQueryUsageExtractor
from metaphor.common.cli import cli_main


def main(config_file: str):
    cli_main(BigQueryUsageExtractor, config_file)
