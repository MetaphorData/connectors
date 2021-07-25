try:
    import unzip_requirements  # noqa: F401
except ImportError:
    pass

from metaphor.common.handler import handle_api
from metaphor.slack_directory.extractor import SlackExtractor, SlackRunConfig


def handle(event, context):
    return handle_api(event, context, SlackRunConfig, SlackExtractor)
