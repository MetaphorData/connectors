import argparse
from typing import Type

from metaphor.common.base_config import BaseConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import EventUtil
from metaphor.common.runner import run_connector


def parse_args():
    parser = argparse.ArgumentParser(description="Metaphor Connectors")

    parser.add_argument(
        "name", help="Name of the connector, e.g. snowflake or bigquery"
    )
    parser.add_argument("config", help="Path to the config file")
    args = parser.parse_args()
    return args


def cli_main(extractor_cls: Type[BaseExtractor], config_file: str):
    base_config = BaseConfig.from_yaml_file(config_file)

    def make_connector():
        """
        Function to create the actual connector
        """
        return extractor_cls.from_config_file(config_file)

    return run_connector(
        # We can't pass the connector instance here or we won't catch any error in connector.__init__
        make_connector=make_connector,
        name=EventUtil.class_fqcn(extractor_cls),
        description=extractor_cls._description,
        platform=extractor_cls._platform,
        file_sink_config=base_config.output.file,
    )
