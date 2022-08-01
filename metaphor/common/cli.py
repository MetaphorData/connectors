from typing import Type

from metaphor.common.extractor import BaseExtractor


def cli_main(extractor_cls: Type[BaseExtractor], config_file: str):
    extractor = extractor_cls()
    config_cls = extractor.config_class()
    extractor.run(config=config_cls.from_yaml_file(config_file))
