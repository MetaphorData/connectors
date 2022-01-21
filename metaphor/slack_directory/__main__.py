from metaphor.common.cli import cli_main

from .extractor import SlackExtractor

if __name__ == "__main__":
    cli_main("Slack directory extractor", SlackExtractor)
