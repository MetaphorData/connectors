import argparse
import logging
import sys
from typing import Type

from .extractor import BaseExtractor, RunConfig


def cli_main(
    description: str, config_cls: Type[RunConfig], extractor_cls: Type[BaseExtractor]
):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s - %(message)s", level=logging.INFO
    )

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config", help="Path to the config file")
    args = parser.parse_args()

    extractor = extractor_cls()
    if not extractor.run(config=config_cls.from_json_file(args.config)):
        sys.exit(1)
