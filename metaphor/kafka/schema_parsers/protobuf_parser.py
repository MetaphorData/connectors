from enum import Enum
import importlib
from pathlib import Path
import shutil
import subprocess
import sys
from typing import List, Optional
from metaphor.models.metadata_change_event import SchemaField

from metaphor.common.logger import get_logger

logger = get_logger()


def snake_to_pascal(name: str) -> str:
    return "".join([tok.capitalize() for i, tok in enumerate(name.lower().split("_"))])

class ProtobufDataTypes(Enum):
    """
    Enum for Protobuf Datatypes.
    
    Reference:
    https://github.com/protocolbuffers/protobuf/blob/main/python/google/protobuf/descriptor.py#L485
    """
    TYPE_DOUBLE         = 1
    TYPE_FLOAT          = 2
    TYPE_INT64          = 3
    TYPE_UINT64         = 4
    TYPE_INT32          = 5
    TYPE_FIXED64        = 6
    TYPE_FIXED32        = 7
    TYPE_BOOL           = 8
    TYPE_STRING         = 9
    TYPE_GROUP          = 10
    TYPE_MESSAGE        = 11
    TYPE_BYTES          = 12
    TYPE_UINT32         = 13
    TYPE_ENUM           = 14
    TYPE_SFIXED32       = 15
    TYPE_SFIXED64       = 16
    TYPE_SINT32         = 17
    TYPE_SINT64         = 18
    MAX_TYPE            = 18

    def __new__(cls, *values):
        obj = object.__new__(cls)
        # first value is canonical value
        obj._value_ = values[0]
        for other_value in values[1:]:
            cls._value2member_map_[other_value] = obj
        obj._all_values = values
        return obj

    def __repr__(self):
        value = ", ".join([repr(v) for v in self._all_values])
        return (
            f"<"  # pylint: disable=no-member
            f"{self.__class__.__name__,}"
            f"{self._name_}"
            f"{value}"
            f">"
        )

class ProtobufParser:
    def __init__(self) -> None:
        self.base_dir = "/tmp/kafka_protobuf_parser"
        self.generated_dir = Path(self.base_dir) / "generated"
        self.proto_interface_dir = Path(self.base_dir) / "proto_interface"

    def _create_proto_file(self, raw_schema: str, schema_name: str) -> Path:
        self.generated_dir.mkdir(parents=True, exist_ok=True)
        self.proto_interface_dir.mkdir(parents=True, exist_ok=True)
        file_path = self.proto_interface_dir / (schema_name + ".proto")
        with open(file_path.as_posix(), "w") as file:
            file.write(raw_schema)
        return file_path

    def _get_protobuf_python_object(self, file_path: Path, schema_name: str):
        # Instead of using grpc_tool.protoc.main, we directly invoke so that
        # we don't need to specify a bunch of imports.
        subprocess.run([
            sys.executable, # Use this or can't run pytest properly
            "-m",
            "grpc_tools.protoc",
            f"--proto_path=generated={self.proto_interface_dir.as_posix()}",
            f"--python_out={self.base_dir}",
            file_path.as_posix(),
        ])

        sys.path.append(self.generated_dir.as_posix())
        py_file = self.generated_dir / f"{schema_name}_pb2.py"
        module_name = py_file.stem
        message = importlib.import_module(module_name)
        class_ = getattr(message, snake_to_pascal(schema_name))
        instance = class_()
        return instance
        
    def _parse_protobuf_fields(self, fields, cur_path: str) -> List[SchemaField]:
        schema_fields = []
        for field in fields:

            field_path = ".".join([cur_path, field.name])
            if cur_path == "":
                field_path = field_path[1:]

            schema_field = SchemaField(
                field_name=field.name,
                field_path=field_path,
                native_type=ProtobufDataTypes(field.type).name,
                subfields=(
                    self._parse_protobuf_fields(field.message_type.fields, field_path)
                    if field.type == 11
                    else None
                )
            )
            schema_fields.append(schema_field)

        return schema_fields

    def run_parser(self, raw_schema: str, subject: str) -> List[SchemaField]:
        file_path = self._create_proto_file(raw_schema, subject)
        instance = self._get_protobuf_python_object(file_path, subject)
        schema_fields = self._parse_protobuf_fields(instance.DESCRIPTOR.fields, cur_path="")

        if Path(self.base_dir).exists():
            shutil.rmtree(self.base_dir)

        return schema_fields

    @staticmethod
    def parse(raw_schema: str, subject: str) -> Optional[List[SchemaField]]:
        try:
            return ProtobufParser().run_parser(raw_schema, subject)
        except Exception:
            logger.exception(f"Failed to parse schema for subject: {subject}")