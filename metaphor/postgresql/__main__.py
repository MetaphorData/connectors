from metaphor.common.cli import cli_main

from .extractor import PostgresqlExtractor, PostgresqlRunConfig

if __name__ == "__main__":
    cli_main("PostgreSQL metadata extractor", PostgresqlRunConfig, PostgresqlExtractor)
