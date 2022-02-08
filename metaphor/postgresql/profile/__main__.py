from metaphor.common.cli import cli_main

from .extractor import PostgreSQLProfileExtractor

if __name__ == "__main__":
    cli_main("PostgreSQL data profile extractor", PostgreSQLProfileExtractor)
