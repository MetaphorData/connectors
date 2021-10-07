from metaphor.common.cli import cli_main

from .extractor import PostgreSQLExtractor, PostgreSQLRunConfig

if __name__ == "__main__":
    cli_main("PostgreSQL metadata extractor", PostgreSQLRunConfig, PostgreSQLExtractor)
