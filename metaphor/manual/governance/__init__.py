from metaphor.common.cli import cli_main
from metaphor.manual.governance.extractor import ManualGovernanceExtractor


def main(config_file: str):
    cli_main(ManualGovernanceExtractor, config_file)
