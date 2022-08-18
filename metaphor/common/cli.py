from typing import Type

from metaphor.common.base_extractor import BaseExtractor


def cli_main(extractor_cls: Type[BaseExtractor], config_file: str):
    extractor = extractor_cls.from_config_file(config_file)
    extractor.run()
