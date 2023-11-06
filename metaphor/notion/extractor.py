import json
import requests
import datetime
import traceback
from typing import Collection
from requests.exceptions.RequestException import HTTPError

from llama_index import download_loader
from llama_index import Document

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform # this needs to be updated with a Notion entry
from metaphor.notion.config import NotionRunConfig

logger = get_logger()

baseurl = 'https://api.notion.com/v1/'

class NotionExtractor(BaseExtractor):
    """Notion Document extractor."""

    @staticmethod
    def from_config_file(config_file: str) -> "NotionExtractor":
        return NotionExtractor(NotionRunConfig.from_yaml_file(config_file))

    def __init__(self, config: NotionRunConfig):
        super().__init__(config, "Notion document crawler", Platform.NOTION)
        self.notion_tok = config.api_key_token
        self.notion_ver = config.api
        
        # Set up LlamaIndex Notion integration
        npr = download_loader('NotionPageReader')
        self.NotionReader = npr(integration_token=self.notion_tok)
        
    async def extract(self) -> Collection(Document):
        logger.info("Fetching documents from Notion")
        # this method should do all the things
        dbs = self._get_databases()
        docs = self._get_all_documents()

        return docs

    def _get_databases(self) -> Collection[str]:
        '''
        Returns a list of database IDs.
        '''

        headers = {'Authorization': f'Bearer {self.notion_tok}',
                'Notion-Version': f'{self.notion_ver}'}

        payload = {
            'query': '',
            'filter': {
                'value': 'database',
                'property': 'object'
            }
        }
        try:
            # send request
            r = requests.post(baseurl + "search",
                            headers=headers,
                            json=payload)
            
            # throw error if 400
            r.raise_for_status()

            # load json
            dbs = json.loads(r.content)['results']

            logger.info(f'Retrieved {len(dbs)} databases')
            
        except HTTPError as error:
            traceback.print_exc()
            logger.error(f'Failed to get Notion database IDs, error {error}')
        

        self.db_ids = [dbs[i]['id'] for i in range(len(dbs))]

        return self.db_ids
    
    
    def _get_all_documents(self) -> Collection[Document]:
        '''
        Returns a list of Documents
        retrieved from a given Notion space.

        The Document metadata will contain 'platform':'notion' and
        the Document's URL.

        get_databases has to have been called previously.
        '''

        self.docs = list()

        # add all documents
        for db_id in self.db_ids:
            logger.info(f"Getting documents from db {db_id}")

            try:
                queried = self.NotionReader.load_data(database_id = db_id)

            except Exception:
                logger.warning(f"Failed to get documents from db {db_id}")

            # exclude functionality would be implemented here somewhere
            
            # update queried document metadata with db_id, platform info, link
            for q in queried:

                # update db_id and platform
                q.metadata['dbId'] = db_id.replace('-', '') # remove hyphens
                q.metadata['platform'] = 'notion'

                # reset page-id, remove hyphens
                q.metadata['pageId'] = q.metadata.pop('page_id').replace('-', '') 

                # construct link
                link = f'https://notion.so/{q.metadata["pageId"]}'
                q.metadata['link'] = link

                # add timestamp (UTC) for crawl-time
                q.metadata['lastRefreshed'] = str(datetime.datetime.utcnow())

            # extend documents
            logger.info(f"Successfully retrieved {len(queried)} documents")
            self.docs.extend(queried)

        return self.docs
