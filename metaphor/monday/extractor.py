import datetime
import json
from typing import Collection, Dict, List, Tuple

import requests
from llama_index.core import Document
from requests.exceptions import HTTPError, RequestException

from metaphor.common.base_extractor import BaseExtractor
from metaphor.common.embeddings import embed_documents, map_metadata, sanitize_text
from metaphor.common.event_util import ENTITY_TYPES
from metaphor.common.logger import get_logger
from metaphor.models.crawler_run_metadata import Platform
from metaphor.monday.config import MondayRunConfig

logger = get_logger()

baseURL = "https://api.monday.com/v2"

embedding_chunk_size = 512
embedding_overlap_size = 50
max_items_query = 500


class MondayExtractor(BaseExtractor):
    """Monday.com Items extractor."""

    _description = "Monday.com board items crawler"
    _platform = Platform.UNKNOWN

    @staticmethod
    def from_config_file(config_file: str) -> "MondayExtractor":
        return MondayExtractor(MondayRunConfig.from_yaml_file(config_file))

    def __init__(self, config: MondayRunConfig):
        super().__init__(config=config)  # type: ignore[call-arg]

        self.monday_api_key = config.monday_api_key
        self.monday_api_version = config.monday_api_version

        self.azure_openAI_key = config.azure_openAI_key
        self.azure_openAI_version = config.azure_openAI_version
        self.azure_openAI_endpoint = config.azure_openAI_endpoint
        self.azure_openAI_model = config.azure_openAI_model
        self.azure_openAI_model_name = config.azure_openAI_model_name

        self.include_text = config.include_text

        # Set default headers for API requests
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": self.monday_api_key,
            "API-Version": self.monday_api_version,
        }

    async def extract(self) -> Collection[ENTITY_TYPES]:
        logger.info("Fetching items from Monday.com")

        self.documents = []

        self.boards = self._get_available_boards()
        self.board_columns = self._parse_board_columns()  # type: ignore[assignment]

        for board_id, board_name in self.boards:
            board_columns = self.board_columns[board_id]

            logger.info(f"Processing board {board_id}:{board_name}")

            board_items = self._get_board_items(board_id, board_columns)
            board_docs = self._construct_items_documents(
                board_items, board_columns, board_id, board_name
            )

            self.documents.extend(board_docs)  # type: ignore[call-arg]

        logger.info("Starting embedding process")

        vsi = embed_documents(
            self.documents,
            self.azure_openAI_key,
            self.azure_openAI_version,
            self.azure_openAI_endpoint,
            self.azure_openAI_model,
            self.azure_openAI_model_name,
            embedding_chunk_size,
            embedding_overlap_size,
        )

        embedded_nodes = map_metadata(vsi, include_text=self.include_text)

        return embedded_nodes

    def _get_available_boards(
        self,
    ) -> List[Tuple[str, str]]:
        """
        Discovers all available Monday.com boards to minimize manual configuration.
        """

        # Retrieve all accessible boards and board metadata
        query = """
                    query {
                    boards {
                        id
                        name
                        columns {
                            id
                            title
                            type
                            }
                        }
                    }
                """
        data = {"query": query}

        try:
            logger.info("Retrieving all available boards")
            r = requests.post(url=baseURL, json=data, headers=self.headers, timeout=15)
            r.raise_for_status()

        except (HTTPError, RequestException) as error:
            logger.warning("Failed to get boards.")
            raise error

        self.boards_metadata = r.json()["data"]["boards"]

        # Return board ids and names only, preserve content
        return [(board["id"], board["name"]) for board in self.boards_metadata]

    def _parse_board_columns(
        self,
        valid_types: list = ["file", "doc", "dropdown", "longtext", "text", "name"],
    ) -> Dict[str, Dict[str, str]]:
        """
        Parses column metadata from boards metadata query response.
        Returns a dictionary of form {board_id: {col1_id: col1_title, col2_id: col2_title}}
        """

        board_columns = dict()  # type: ignore[var-annotated]

        for board in self.boards_metadata:
            # set board id as key, empty dict for columns as value
            board_columns[board["id"]] = dict()

            for col in board["columns"]:
                if col["type"] in set(valid_types):
                    board_columns[board["id"]][col["id"]] = col["title"]

        return board_columns

    def _get_board_items(
        self,
        board_id: str,
        columns: dict,
        params: str = "{}",
        consume: bool = True,
    ) -> Collection[dict]:
        """
        Retrieves max_items items from specified board.
        If consume == True and a cursor is present,
            uses consume_items_page() to get all remaining items available to the cursor.
        """

        column_ids = list(columns.keys())

        query = f"""
                query {{
                    boards(ids: [{board_id}]) {{
                        items_page(query_params:{params}, limit:{max_items_query}) {{
                            cursor
                            items {{
                                id
                                name
                                updates {{
                                    text_body
                                }}
                                column_values(ids: {json.dumps(column_ids)}) {{
                                    id
                                    text
                                    value
                                }}
                                url
                            }}
                        }}
                    }}
                }}
                """
        data = {"query": query}

        try:
            logger.info(f"Getting items for board {board_id}")
            r = requests.post(url=baseURL, json=data, headers=self.headers, timeout=30)
            r.raise_for_status()

        except (HTTPError, RequestException) as error:
            logger.warning(f"Failed to get items for board {board_id}, err: {error}")

        content = r.json()["data"]["boards"][0]

        cursor = content["items_page"]["cursor"]
        items = content["items_page"]["items"]

        if consume and cursor:
            items = self._consume_items_cursor(cursor, items, column_ids, board_id)

        return items

    def _consume_items_cursor(
        self, cursor: str, items: Collection[dict], column_ids: list, board_id: str
    ) -> Collection[dict]:
        query = f"""
                query {{
                    next_items_page (limit: {max_items_query}, cursor: "{cursor}") {{
                        cursor
                        items {{
                                id
                                name
                                updates {{
                                    text_body
                                }}
                                column_values(ids: {json.dumps(column_ids)}) {{
                                    id
                                    text
                                    value
                                }}
                                url
                        }}
                    }}
                }}
                """

        data = {"query": query}

        try:
            logger.info(f"Consuming cursor {cursor} for board {board_id}")
            r = requests.post(url=baseURL, json=data, headers=self.headers, timeout=30)
            r.raise_for_status()

        except (HTTPError, RequestException) as error:
            logger.warning(
                f"Failed to get items for board {board_id} with cursor {cursor}, err: {error}"
            )

        content = r.json()["data"]["next_items_page"]

        cursor = content["cursor"]
        new_items = content["items"]

        items.extend(new_items)  # type: ignore[attr-defined]

        if cursor:
            return self._consume_items_cursor(cursor, items, column_ids, board_id)
        else:
            return items

    def _get_monday_doc(self, object_id: int) -> str:
        query = f"""
                    {{
                    docs (object_ids:{object_id}) {{
                        blocks {{
                        content
                        }}
                    }}
                    }}
                """
        data = {"query": query}

        try:
            logger.info(f"Retrieving Monday doc {object_id}")
            r = requests.post(url=baseURL, json=data, headers=self.headers, timeout=15)
            r.raise_for_status()

        except (HTTPError, RequestException) as error:
            logger.warning(f"Failed to get Monday doc {object_id}, err: {error}")

        content = r.json()["data"]["docs"][0]

        blocks = content["blocks"]

        document_string = ""

        for block in blocks:
            block_json = json.loads(block["content"])
            if block_json.get("deltaFormat"):  # If there's a text body present
                block_text = block_json["deltaFormat"][0]["insert"]
                document_string += f"{block_text}\n"

        return document_string

    def _construct_items_documents(
        self, items: Collection[dict], columns: dict, board_id: str, board_name: str
    ):
        """
        Constructs Document objects from a list of items and column metadata.
        If a column contains a Monday document, calls get_monday_doc() to
            pull the document text into the item Document.
        """
        time = str(datetime.datetime.now())
        documents = []

        # Construct Document for each item collected
        for item in items:
            item_id = item["id"]
            item_name = item["name"]
            item_url = item["url"]
            item_text_string = ""

            updates = item["updates"]
            updates_text = [u["text_body"] for u in updates]

            item_text_string += f"Board Name: {board_name}\n"

            if updates_text:
                for update in updates_text:
                    item_text_string += f"Update: {sanitize_text(update)}\n"

            for column in item["column_values"]:
                column_name = columns[column["id"]]
                text = column["text"]

                if (
                    "monday_doc" in column["id"] and column["value"]
                ):  # Get content of embedded Monday doc
                    object_id = json.loads(column["value"])["files"][0]["objectId"]
                    doc_content = self._get_monday_doc(object_id)
                    item_text_string += f"{column_name}: {sanitize_text(doc_content)}\n"
                elif "file" in column["id"]:
                    pass  # Implement file handling? (TODO)
                else:
                    if text:
                        item_text_string += f"{column_name}: {sanitize_text(text)}\n"

            metadata = {
                "title": item_name,
                "board": board_id,
                "boardName": board_name,
                "link": item_url,
                "pageId": item_id,
                "platform": "monday",
                "lastRefreshed": time,
            }

            documents.append(Document(text=item_text_string, extra_info=metadata))

        return documents
