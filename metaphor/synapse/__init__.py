from metaphor.common.cli import cli_main
from metaphor.synapse.extractor import SynapseExtractor


def main(config_file: str):
    cli_main(SynapseExtractor, config_file)
