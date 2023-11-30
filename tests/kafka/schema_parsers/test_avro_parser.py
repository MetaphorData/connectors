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
                        },
                        {
                            "name": "accountOwner",
                            "type": {
                                "name": "OwnerRecord",
                                "type": "record",
                                "fields": [
                                    {
                                        "type": "string",
                                        "name": "name"
                                    },
                                    {
                                        "type": "string",
                                        "name": "email"
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        }
    ]
}
    """
    schema = AvroParser.parse(raw_schema, "account-event")
    assert schema == [
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
                    native_type="ARRAY<record>",
                    subfields=[
                        SchemaField(
                            field_name="Account",
                            field_path="AccountEvent.accountList.Account",
                            native_type="RECORD",
                            subfields=[
                                SchemaField(
                                    field_name="accountNumber",
                                    field_path="AccountEvent.accountList.Account.accountNumber",
                                    native_type="STRING",
                                ),
                                SchemaField(
                                    field_name="id",
                                    field_path="AccountEvent.accountList.Account.id",
                                    native_type="STRING",
                                ),
                                SchemaField(
                                    field_name="accountOwner",
                                    field_path="AccountEvent.accountList.Account.accountOwner",
                                    native_type="RECORD",
                                    subfields=[
                                        SchemaField(
                                            field_name="OwnerRecord",
                                            field_path="AccountEvent.accountList.Account.accountOwner.OwnerRecord",
                                            native_type="RECORD",
                                            subfields=[
                                                SchemaField(
                                                    field_name="name",
                                                    field_path="AccountEvent.accountList.Account.accountOwner.OwnerRecord.name",
                                                    native_type="STRING",
                                                ),
                                                SchemaField(
                                                    field_name="email",
                                                    field_path="AccountEvent.accountList.Account.accountOwner.OwnerRecord.email",
                                                    native_type="STRING",
                                                ),
                                            ],
                                        )
                                    ],
                                ),
                            ],
                        )
                    ],
                ),
            ],
        )
    ]


def test_parse_union_schema() -> None:
    raw_schema = """
    {
        "type": "record",
        "namespace": "com.example",
        "name": "FullName",
        "fields": [
            { "name": "first", "type": ["string", "null"] },
            { "name": "last", "type": "string", "default" : "Doe" }
        ]
    }
    """
    schema = AvroParser.parse(raw_schema, "FullName")
    assert schema == [
        SchemaField(
            field_name="FullName",
            field_path="FullName",
            native_type="RECORD",
            subfields=[
                SchemaField(
                    field_name="first",
                    field_path="FullName.first",
                    native_type="UNION<string,null>",
                ),
                SchemaField(
                    field_name="last",
                    field_path="FullName.last",
                    native_type="STRING",
                ),
            ],
        )
    ]


def test_parse_nested_array_schema() -> None:
    # Nested arrays go brrrrr
    raw_schema = """
    {
        "name": "Top",
        "type": "record",
        "fields": [
            {
                "name": "FirstLayer",
                "type": {
                    "type": "array",
                    "items": {
                        "name": "SecondLayer",
                        "type": "array",
                        "items": {
                            "name": "ThirdLayer",
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "Leaf",
                                "fields": [
                                    {
                                        "name": "id",
                                        "type": "string"
                                    },
                                    {
                                        "name": "LeafArray",
                                        "type": {
                                            "type": "array",
                                            "items": {
                                                "name": "LeafNested",
                                                "type": "array",
                                                "items": {
                                                    "type": "string"
                                                }
                                            }
                                        }
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        ]
    }
    """
    schema = AvroParser.parse(raw_schema, "bunch-of-arrays")
    assert schema == [
        SchemaField(
            field_name="Top",
            field_path="Top",
            native_type="RECORD",
            subfields=[
                SchemaField(
                    field_name="FirstLayer",
                    field_path="Top.FirstLayer",
                    native_type="ARRAY<ARRAY<ARRAY<record>>>",
                    subfields=[
                        SchemaField(
                            field_name="Leaf",
                            field_path="Top.FirstLayer.Leaf",
                            native_type="RECORD",
                            subfields=[
                                SchemaField(
                                    field_name="id",
                                    field_path="Top.FirstLayer.Leaf.id",
                                    native_type="STRING",
                                ),
                                SchemaField(
                                    field_name="LeafArray",
                                    field_path="Top.FirstLayer.Leaf.LeafArray",
                                    native_type="ARRAY<ARRAY<string>>",
                                ),
                            ],
                        )
                    ],
                )
            ],
        )
    ]


def test_complex_schema() -> None:
    raw_schema = """
    {
        "namespace": "proj.avro",
        "protocol": "app_messages",
        "doc" : "application messages",
        "name": "myRecord",
        "type" : "record",
        "fields": [
            {
                "name": "requestResponse",
                "type": [
                    {
                        "name": "record_request",
                        "type" : "record",
                        "fields": [
                            {
                                "name" : "request_id",
                                "type" : "int"
                            },
                            {
                                "name" : "message_type",
                                "type" : "int"
                            },
                            {
                                "name" : "users",
                                "type" : "string"
                            }
                        ]
                    },
                    {
                        "name" : "request_response",
                        "type" : "record",
                        "fields": [
                            {
                                "name" : "request_id",
                                "type" : "int"
                            },
                            {
                                "name" : "response_code",
                                "type" : "string"
                            },
                            {
                                "name" : "response_count",
                                "type" : "int"
                            },
                            {
                                "name" : "reason_code",
                                "type" : "string"
                            }
                        ]
                    }
                ]
            }
        ]
    }
    """
    schema = AvroParser.parse(raw_schema, "complex")
    assert schema == [
        SchemaField(
            description="application messages",
            field_name="myRecord",
            field_path="myRecord",
            native_type="RECORD",
            subfields=[
                SchemaField(
                    field_name="requestResponse",
                    field_path="myRecord.requestResponse",
                    native_type="UNION<record,record>",  # FIXME Where are the subfields?
                )
            ],
        )
    ]
