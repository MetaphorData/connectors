from metaphor.common.cli import cli_main

from .extractor import GoogleDirectoryExtractor

if __name__ == "__main__":
    cli_main("Google directory connector", GoogleDirectoryExtractor)
