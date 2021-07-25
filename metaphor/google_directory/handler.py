try:
    import unzip_requirements  # noqa: F401
except ImportError:
    pass

from metaphor.common.handler import handle_api

from .extractor import GoogleDirectoryExtractor, GoogleDirectoryRunConfig


def handle(event, context):
    return handle_api(
        event, context, GoogleDirectoryRunConfig, GoogleDirectoryExtractor
    )
