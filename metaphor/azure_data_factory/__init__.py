from metaphor.azure_data_factory.extractor import AzureDataFactoryExtractor
from metaphor.common.cli import cli_main


def main(config_file: str):
    cli_main(AzureDataFactoryExtractor, config_file)
