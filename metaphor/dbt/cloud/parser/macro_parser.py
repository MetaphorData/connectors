from typing import Dict, List

from metaphor.dbt.cloud.discovery_api import DiscoveryAPIClient
from metaphor.dbt.cloud.discovery_api.graphql_client.get_job_run_macros import (
    GetJobRunMacrosJobMacros,
)
from metaphor.dbt.cloud.discovery_api.graphql_client.input_types import (
    MacroDefinitionFilter,
)
from metaphor.models.metadata_change_event import DbtMacro, DbtMacroArgument


class MacroParser:
    def __init__(
        self,
        discovery_api: DiscoveryAPIClient,
    ) -> None:
        self._discovery_api = discovery_api

    def parse(self, macros: List[GetJobRunMacrosJobMacros]) -> Dict[str, DbtMacro]:
        macro_map: Dict[str, DbtMacro] = {}
        for macro in macros:
            definition = self._discovery_api.get_macro_arguments(
                macro.environment_id, MacroDefinitionFilter(uniqueIds=[macro.unique_id])
            ).environment.definition
            if not definition:
                continue

            arguments = []
            if len(definition.macros.edges) == 1:
                macro_definition = definition.macros.edges[0].node
                arguments = [
                    DbtMacroArgument(
                        name=arg.name,
                        type=arg.type,
                        description=arg.description,
                    )
                    for arg in macro_definition.arguments
                ]

            macro_depends_on = [x for x in macro.depends_on if x.startswith("macro.")]
            macro_map[macro.unique_id] = DbtMacro(
                name=macro.name,
                unique_id=macro.unique_id,
                package_name=macro.package_name,
                description=macro.description,
                arguments=arguments,
                sql=macro.macro_sql,
                depends_on_macros=macro_depends_on if macro_depends_on else None,
            )

        return macro_map
