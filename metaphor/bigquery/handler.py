try:
    import unzip_requirements  # noqa: F401
except ImportError:
    pass

from metaphor.bigquery.extractor import BigQueryExtractor, BigQueryRunConfig
from metaphor.common.handler import handle_api


def handle(event, context):
    return handle_api(event, context, BigQueryRunConfig, BigQueryExtractor)
