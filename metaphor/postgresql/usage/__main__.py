from metaphor.common.cli import cli_main

from .extractor import PostgreSQLUsageExtractor

if __name__ == "__main__":
    cli_main("PostgreSQL usage metadata extractor", PostgreSQLUsageExtractor)
