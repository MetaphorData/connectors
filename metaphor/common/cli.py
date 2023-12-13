from typing import Type

from metaphor.common.base_config import BaseConfig
from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.event_util import EventUtil
from metaphor.common.runner import run_connector


def cli_main(extractor_cls: Type[BaseExtractor], config_file: str):
    base_config = BaseConfig.from_yaml_file(config_file)

    def connector_func():
        extractor = extractor_cls.from_config_file(config_file)
        return extractor.run_async()

    return run_connector(
        connector_func=connector_func,
        name=EventUtil.class_fqcn(extractor_cls),
        description=extractor_cls._description,
        platform=extractor_cls._platform,
        file_sink_config=base_config.output.file,
    )
