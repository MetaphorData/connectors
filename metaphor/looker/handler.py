try:
    import unzip_requirements  # noqa: F401
except ImportError:
    pass

from metaphor.common.handler import handle_api
from metaphor.looker.extractor import LookerExtractor, LookerRunConfig


def handle(event, context):
    return handle_api(event, context, LookerRunConfig, LookerExtractor)
