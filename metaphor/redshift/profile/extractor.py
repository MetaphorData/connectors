from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.models.metadata_change_event import DataPlatform
from metaphor.postgresql.profile.extractor import PostgreSQLProfileExtractor
from metaphor.redshift.profile.config import RedshiftProfileRunConfig

logger = get_logger(__name__)


class RedshiftProfileExtractor(PostgreSQLProfileExtractor):
    """Redshift data profile extractor"""

    @staticmethod
    def from_config_file(config_file: str) -> "RedshiftProfileExtractor":
        return RedshiftProfileExtractor(
            RedshiftProfileRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: RedshiftProfileRunConfig):
        super().__init__(
            config,
            "Redshift data profile crawler",
            Platform.REDSHIFT,
            DataPlatform.REDSHIFT,
        )
