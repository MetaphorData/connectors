from metaphor.common.cli import cli_main

from .extractor import LookerExtractor

if __name__ == "__main__":
    cli_main("Looker metadata extractor", LookerExtractor)
