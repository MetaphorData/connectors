try:
    import unzip_requirements  # noqa: F401
except ImportError:
    pass

from metaphor.common.handler import handle_api
from metaphor.snowflake_extractor.extractor import (
    SnowflakeExtractor,
    SnowflakeRunConfig,
)


def handle(event, context):
    return handle_api(event, context, SnowflakeRunConfig, SnowflakeExtractor)
