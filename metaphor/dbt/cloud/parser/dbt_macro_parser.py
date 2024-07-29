from collections import defaultdict
from typing import Dict, List

from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.generated.get_job_run_macros import (
    GetJobRunMacrosJobMacros,
)
from metaphor.dbt.cloud.discovery_api.generated.get_macro_arguments import (
    GetMacroArgumentsEnvironmentDefinitionMacrosEdges as GetMacroArgumentsEdge,
)
from metaphor.dbt.cloud.discovery_api.generated.input_types import MacroDefinitionFilter
from metaphor.models.metadata_change_event import DbtMacro, DbtMacroArgument


class MacroParser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
    ) -> None:
        self._discovery_api = discovery_api

    def _parse_macro_arguments(self, macros: List[GetJobRunMacrosJobMacros]):
        macros_by_environment = defaultdict(list)
        for macro in macros:
            macros_by_environment[macro.environment_id].append(macro.unique_id)

        arguments: Dict[str, List[DbtMacroArgument]] = dict()
        for environment_id, unique_ids in macros_by_environment.items():
            edges: List[GetMacroArgumentsEdge] = []
            after = None
            while True:
                environment = self._discovery_api.get_macro_arguments(
                    environment_id,
                    filter=MacroDefinitionFilter(
                        uniqueIds=unique_ids,
                    ),
                    after=after,
                ).environment
                definition = environment.definition
                if not definition:
                    break
                edges += definition.macros.edges
                after = definition.macros.page_info.end_cursor
                if not definition.macros.page_info.has_next_page:
                    break

            for edge in edges:
                node = edge.node
                arguments[node.unique_id] = [
                    DbtMacroArgument(
                        name=arg.name,
                        type=arg.type,
                        description=arg.description,
                    )
                    for arg in node.arguments
                ]
        return arguments

    def parse(self, macros: List[GetJobRunMacrosJobMacros]) -> Dict[str, DbtMacro]:
        arguments = self._parse_macro_arguments(macros)

        macro_map: Dict[str, DbtMacro] = {}
        for macro in macros:
            macro_depends_on = [x for x in macro.depends_on if x.startswith("macro.")]
            macro_map[macro.unique_id] = DbtMacro(
                name=macro.name,
                unique_id=macro.unique_id,
                package_name=macro.package_name,
                description=macro.description,
                arguments=arguments.get(macro.unique_id),
                sql=macro.macro_sql,
                depends_on_macros=macro_depends_on if macro_depends_on else None,
            )

        return macro_map
