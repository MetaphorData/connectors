from dataclasses import dataclass
from typing import List

import tableauserverclient as tableau

from metaphor.common.logger import get_logger, json_dump_to_debug_file
from metaphor.tableau.graphql_utils import fetch_workbooks
from metaphor.tableau.query import WorkbookQueryResponse

logger = get_logger()


@dataclass
class Workbook:
    rest_item: tableau.WorkbookItem
    graphql_item: WorkbookQueryResponse

    @property
    def project_id(self) -> str:
        return self.graphql_item.projectVizportalUrlId

    @property
    def id(self) -> str:
        return self.graphql_item.luid

    @property
    def project_name(self) -> str:
        return self.graphql_item.projectName


def get_all_workbooks(server: tableau.Server, batch_size: int):
    graphql_items = fetch_workbooks(server, batch_size)
    rest_items: List[tableau.WorkbookItem] = list(tableau.Pager(server.workbooks))
    workbooks: List[Workbook] = list()
    for rest_item in rest_items:
        graphql_item = next(
            (
                graphql_item
                for graphql_item in graphql_items
                if graphql_item.luid == rest_item.id
            ),
            None,
        )
        if graphql_item:
            workbooks.append(Workbook(rest_item=rest_item, graphql_item=graphql_item))
    json_dump_to_debug_file([w.rest_item.__dict__ for w in workbooks], "workbooks.json")
    logger.info(
        f"\nThere are {len(workbooks)} workbooks on site: {[workbook.rest_item.name for workbook in workbooks]}"
    )
    return workbooks
