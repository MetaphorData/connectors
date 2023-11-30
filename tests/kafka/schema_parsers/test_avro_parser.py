from metaphor.kafka.schema_parsers.avro_parser import AvroParser
from metaphor.models.metadata_change_event import SchemaField


def test_parse_schema() -> None:
    raw_schema = """
{
    "type": "record",
    "namespace": "com.mypackage",
    "name": "AccountEvent",
    "fields": [
        {
            "name": "accountNumber",
            "type": "string"
        },
        {
            "name": "address",
            "type": "string"
        },
        {
            "name": "accountList",
            "type": {
                "type": "array",
                "items":{
                    "name": "Account",
                    "type": "record",
                    "fields":[
                        {   "name": "accountNumber",
                            "type": "string"
                        },
                        {   "name": "id",
                            "type": "string"
                        }
                    ]
                }
            }
        }
    ]
}
    """
    schema_fields = AvroParser.parse(raw_schema, "account-event")
    assert schema_fields == [
        SchemaField(
            field_name="AccountEvent",
            field_path="AccountEvent",
            native_type="RECORD",
            subfields=[
                SchemaField(
                    field_name="accountNumber",
                    field_path="AccountEvent.accountNumber",
                    native_type="STRING",
                ),
                SchemaField(
                    field_name="address",
                    field_path="AccountEvent.address",
                    native_type="STRING",
                ),
                SchemaField(
                    field_name="accountList",
                    field_path="AccountEvent.accountList",
                    native_type="ARRAY",
                    subfields=[
                        SchemaField(
                            field_name="Account",
                            field_path="AccountEvent.accountList.Account",
                            native_type="RECORD",
                            subfields=[
                                SchemaField(
                                    field_name="accountNumber",
                                    field_path="AccountEvent.accountList.accountNumber",
                                    native_type="STRING",
                                ),
                                SchemaField(
                                    field_name="id",
                                    field_path="AccountEvent.accountList.id",
                                    native_type="STRING",
                                ),
                            ],
                        )
                    ],
                ),
            ],
        )
    ]
