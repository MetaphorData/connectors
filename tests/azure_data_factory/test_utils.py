import azure.mgmt.datafactory.models as DfModels

from metaphor.azure_data_factory.utils import (
    LinkedService,
    process_azure_sql_linked_service,
    safe_get_from_json,
)


def test_safe_get_from_json():
    assert safe_get_from_json(None) is None


def test_process_azure_sql_linked_service():
    data = DfModels.AzureSqlDatabaseLinkedService.deserialize(
        {
            "connectionString": "Data Source=foo.database.windows.net;Initial Catalog=bar;invalid;a=b",
        }
    )
    assert process_azure_sql_linked_service(data, "foo") == LinkedService(
        database="bar", account="foo"
    )
