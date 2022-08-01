from metaphor.common.cli import cli_main
from metaphor.metabase.extractor import MetabaseExtractor


def main(config_file: str):
    cli_main(MetabaseExtractor, config_file)
