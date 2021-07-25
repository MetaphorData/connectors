from metaphor.common.cli import cli_main

from .extractor import GoogleDirectoryExtractor, GoogleDirectoryRunConfig

if __name__ == "__main__":
    cli_main(
        "Google directory connector", GoogleDirectoryRunConfig, GoogleDirectoryExtractor
    )
