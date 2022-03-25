from metaphor.common.cli import cli_main

from .extractor import ManualLineageExtractor

if __name__ == "__main__":
    cli_main("Manual lineage extractor", ManualLineageExtractor)
