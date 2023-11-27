from metaphor.common.cli import cli_main
from metaphor.kafka.extractor import KafkaExtractor


def main(config_file: str):
    cli_main(KafkaExtractor, config_file)
