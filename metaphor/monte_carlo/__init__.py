from metaphor.common.cli import cli_main
from metaphor.monte_carlo.extractor import MonteCarloExtractor


def main(config_file: str):
    cli_main(MonteCarloExtractor, config_file)
