from metaphor.common.logger import get_logger
from metaphor.postgresql.profile.extractor import PostgreSQLProfileExtractor
from metaphor.redshift.profile.config import RedshiftProfileRunConfig
from metaphor.redshift.utils import exclude_system_databases

logger = get_logger()


class RedshiftProfileExtractor(PostgreSQLProfileExtractor):
    """Redshift data profile extractor"""

    _description = "Redshift data profile crawler"

    @staticmethod
    def from_config_file(config_file: str) -> "RedshiftProfileExtractor":
        return RedshiftProfileExtractor(
            RedshiftProfileRunConfig.from_yaml_file(config_file)
        )

    def __init__(self, config: RedshiftProfileRunConfig):
        super().__init__(config)
        self._filter = exclude_system_databases(self._filter)
