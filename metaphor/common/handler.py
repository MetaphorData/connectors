import json
import logging
from typing import Type

from metaphor.common.extractor import BaseExtractor, RunConfig

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handle_api(
    event, context, runConfig: Type[RunConfig], extractor: Type[BaseExtractor]
):
    try:
        return _handle_api(event, context, runConfig, extractor)
    except Exception as e:
        logger.exception(str(e))
        return {"statusCode": 500, "body": str(e)}


def _handle_api(
    event, context, config_cls: Type[RunConfig], extractor_cls: Type[BaseExtractor]
):
    config_file = event.get("config_file", None)
    if config_file is None:
        return {"statusCode": 422, "body": "Missing 'config_file' parameter"}

    try:
        run_config = config_cls.from_json_file(config_file)
    except KeyError as e:
        return {"statusCode": 422, "body": f"Missing {e} key in {config_file}"}

    actor = extractor_cls()
    actor.run(config=run_config)
    return {"statusCode": 200, "body": json.dumps(event)}
