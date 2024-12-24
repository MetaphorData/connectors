from typing import Dict, List

from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.generated.get_macros import (
    GetMacrosEnvironmentDefinitionMacrosEdgesNode as Node,
)
from metaphor.dbt.cloud.parser.common import DISCOVERY_API_PAGE_LIMIT
from metaphor.models.metadata_change_event import DbtMacro, DbtMacroArgument


class MacroParser:
    def __init__(self, discovery_api: DiscoveryAPIClient) -> None:
        self._discovery_api = discovery_api

    def _parse_macros(self, macros: List[Node]) -> Dict[str, DbtMacro]:
        macro_map: Dict[str, DbtMacro] = {}

        for macro in macros:
            macro_map[macro.unique_id] = DbtMacro(
                name=macro.name,
                unique_id=macro.unique_id,
                package_name=macro.package_name,
                description=macro.description,
                sql=macro.macro_sql,
                arguments=[
                    DbtMacroArgument(
                        name=arg.name,
                        type=arg.type,
                        description=arg.description,
                    )
                    for arg in macro.arguments
                ],
            )

        return macro_map

    def _get_macros_in_environment(self, environment_id: int) -> List[Node]:
        macro_nodes: List[Node] = []
        after = None

        while True:
            result = self._discovery_api.get_macros(
                environment_id, first=DISCOVERY_API_PAGE_LIMIT, after=after
            )
            definition = result.environment.definition
            if not definition or not definition.macros:
                break
            macro_nodes += [e.node for e in definition.macros.edges]
            after = definition.macros.page_info.end_cursor
            if not definition.macros.page_info.has_next_page:
                break

        return macro_nodes

    def parse(self, environment_id: int) -> Dict[str, DbtMacro]:
        """
        Fetch and parse macros in a given environment, results are returned as a dict.
        """
        macros = self._get_macros_in_environment(environment_id)
        return self._parse_macros(macros)
