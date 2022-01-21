from metaphor.common.cli import cli_main

from .extractor import MetabaseExtractor

if __name__ == "__main__":
    cli_main("Metabase metadata extractor", MetabaseExtractor)
