from metaphor.common.cli import cli_main

from .extractor import ThoughtspotExtractor

if __name__ == "__main__":
    cli_main("Thoughtspot metadata extractor", ThoughtspotExtractor)
