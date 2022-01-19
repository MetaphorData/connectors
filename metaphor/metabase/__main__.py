from metaphor.common.cli import cli_main

from .extractor import MetabaseExtractor, MetabaseRunConfig

if __name__ == "__main__":
    cli_main("Metabase metadata extractor", MetabaseRunConfig, MetabaseExtractor)
