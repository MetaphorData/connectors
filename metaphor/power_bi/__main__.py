from metaphor.common.cli import cli_main

from .extractor import PowerBIExtractor

if __name__ == "__main__":
    cli_main("Power BI metadata extractor", PowerBIExtractor)
