from metaphor.common.cli import cli_main
from metaphor.s3.extractor import S3Extractor


def main(config_file: str):
    cli_main(S3Extractor, config_file)
