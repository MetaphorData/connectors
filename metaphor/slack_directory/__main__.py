from metaphor.common.cli import cli_main

from .extractor import SlackExtractor, SlackRunConfig

if __name__ == "__main__":
    cli_main("Slack directory extractor", SlackRunConfig, SlackExtractor)
