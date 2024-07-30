from typing import Collection

from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.database.extractor import GenericDatabaseExtractor
from metaphor.models.crawler_run_metadata import Platform
from metaphor.mysql.config import MySQLRunConfig

logger = get_logger()


class MySQLExtractor(GenericDatabaseExtractor):
    """MySQL metadata extractor"""

    _description = "MySQL metadata crawler"
    _platform = Platform.MYSQL
    _sqlalchemy_dialect = "mysql+pymysql"

    @staticmethod
    def from_config_file(config_file: str) -> "MySQLExtractor":
        return MySQLExtractor(MySQLRunConfig.from_yaml_file(config_file))

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info(f"Fetching metadata from MySQL host {self._config.host}")

        inspector = MySQLExtractor.get_inspector(self._get_sqlalchemy_url())
        for schema in self._get_schemas(inspector):
            self._extract_schema(inspector, schema)
        self._extract_foreign_key(inspector)

        return self._datasets.values()
