from metaphor.common.cli import cli_main
from metaphor.custom.governance.extractor import CustomGovernanceExtractor


def main(config_file: str):
    cli_main(CustomGovernanceExtractor, config_file)
