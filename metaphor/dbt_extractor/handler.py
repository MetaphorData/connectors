try:
    import unzip_requirements  # noqa: F401
except ImportError:
    pass

from metaphor.common.handler import handle_api
from metaphor.dbt_extractor.extractor import DbtExtractor, DbtRunConfig


def handle(event, context):
    return handle_api(event, context, DbtRunConfig, DbtExtractor)
