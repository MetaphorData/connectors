import datetime
import json
from typing import Collection

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

        self.boards = config.boards

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

        for board in self.boards:
            self.current_board = board
            logger.info(f"Processing board {board}")

            board_columns = self._get_board_columns(board)
            board_items = self._get_board_items(board, board_columns)
            board_docs = self._construct_items_documents(board_items, board_columns)

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

    def _get_board_columns(
        self,
        board: int,
        valid_types: list = ["file", "doc", "dropdown", "longtext", "text", "name"],
    ) -> dict:
        """
        Gets column metadata from a specified board.
        Returns a dictionary of form (column_id, column_title)
        """

        # Retrieve column data for a specified board
        query = f"""
                    query{{
                    boards(ids: [{board}]) {{
                        name
                        columns {{
                        id
                        title
                        type
                        }}
                    }}
                    }}
                """
        data = {"query": query}

        try:
            logger.info(f"Getting columns for board {board}")
            r = requests.post(url=baseURL, json=data, headers=self.headers, timeout=15)
            r.raise_for_status()

        except (HTTPError, RequestException) as error:
            logger.warning(f"Failed to get columns for board {board}, err: {error}")

        content = r.json()

        columns = dict()
        for col in content["data"]["boards"][0]["columns"]:
            if col["type"] in set(valid_types):
                columns[col["id"]] = col["title"]

        # Extract board name from response
        self.current_board_name = content["data"]["boards"][0]["name"]

        return columns

    def _get_board_items(
        self,
        board: int,
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
                {{
                    boards(ids: [{board}]) {{
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
            logger.info(f"Getting items for board {board}")
            r = requests.post(url=baseURL, json=data, headers=self.headers, timeout=30)
            r.raise_for_status()

        except (HTTPError, RequestException) as error:
            logger.warning(f"Failed to get items for board {board}, err: {error}")

        content = r.json()

        cursor = content["data"]["boards"][0]["items_page"]["cursor"]
        items = content["data"]["boards"][0]["items_page"]["items"]

        if consume and cursor:
            items = self._consume_items_cursor(cursor, items, column_ids)

        return items

    def _consume_items_cursor(
        self, cursor: str, items: Collection[dict], column_ids: list
    ) -> Collection[dict]:
        query = f"""
                {{
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
            logger.info(f"Consuming cursor {cursor} for board {self.current_board}")
            r = requests.post(url=baseURL, json=data, headers=self.headers, timeout=30)
            r.raise_for_status()

        except (HTTPError, RequestException) as error:
            logger.warning(
                f"Failed to get items for board {self.current_board} with cursor {cursor}, err: {error}"
            )

        content = r.json()

        cursor = content["data"]["next_items_page"]["cursor"]
        new_items = content["data"]["next_items_page"]["items"]

        items.extend(new_items)  # type: ignore[attr-defined]

        if cursor:
            return self._consume_items_cursor(cursor, items, column_ids)
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

        content = r.json()

        blocks = content["data"]["docs"][0]["blocks"]

        document_string = ""

        for block in blocks:
            block_json = json.loads(block["content"])
            if block_json.get("deltaFormat"):  # If there's a text body present
                block_text = block_json["deltaFormat"][0]["insert"]
                document_string += f"{block_text}\n"

        return document_string

    def _construct_items_documents(self, items: Collection[dict], columns: dict):
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

            item_text_string += f"Board Name: {self.current_board_name}\n"

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
                "board": self.current_board,
                "boardName": self.current_board_name,
                "link": item_url,
                "pageId": item_id,
                "platform": "monday",
                "lastRefreshed": time,
            }

            documents.append(Document(text=item_text_string, extra_info=metadata))

        return documents
