import argparse
import logging
import os
from typing import Type

from .extractor import BaseExtractor


def cli_main(description: str, extractor_cls: Type[BaseExtractor]):
    logging.basicConfig(
        format="%(asctime)s %(levelname)s - %(message)s", level=logging.INFO
    )

    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("config", help="Path to the config file")
    args = parser.parse_args()

    extractor = extractor_cls()
    config_cls = extractor.config_class()

    extension = os.path.splitext(args.config)[-1].lower()

    if extension == ".json":
        extractor.run(config=config_cls.from_json_file(args.config))
    elif extension == ".yml" or extension == ".yaml":
        extractor.run(config=config_cls.from_yaml_file(args.config))
    else:
        raise ValueError(f"Unknown config file extension: {extension}")
