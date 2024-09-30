from metaphor.common.cli import cli_main
from metaphor.mongodb.extractor import MongoDBExtractor


def main(config_file: str):
    cli_main(MongoDBExtractor, config_file)
