import importlib
import shutil
import subprocess
import sys
import tempfile
from enum import Enum
from pathlib import Path
from typing import List, Optional

from google.protobuf.descriptor import FileDescriptor  # type: ignore

from metaphor.common.fieldpath import build_field_path
from metaphor.common.logger import get_logger
from metaphor.models.metadata_change_event import SchemaField

logger = get_logger()


class ProtobufDataTypes(Enum):
    """
    Enum for Protobuf Datatypes.

    Reference:
    https://github.com/protocolbuffers/protobuf/blob/main/python/google/protobuf/descriptor.py#L485
    """

    TYPE_DOUBLE = 1
    TYPE_FLOAT = 2
    TYPE_INT64 = 3
    TYPE_UINT64 = 4
    TYPE_INT32 = 5
    TYPE_FIXED64 = 6
    TYPE_FIXED32 = 7
    TYPE_BOOL = 8
    TYPE_STRING = 9
    TYPE_GROUP = 10
    TYPE_MESSAGE = 11
    TYPE_BYTES = 12
    TYPE_UINT32 = 13
    TYPE_ENUM = 14
    TYPE_SFIXED32 = 15
    TYPE_SFIXED64 = 16
    TYPE_SINT32 = 17
    TYPE_SINT64 = 18
    MAX_TYPE = 18


class ProtobufParser:
    def __init__(self) -> None:
        self.base_dir = tempfile.mkdtemp()
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
        """
        Compiles the protobuf schema to a Python file, then import the Python
        file dynamically to determine the top level message object.

        The top level message name is parsed internally. If there are more than
        one possible top level message, we use the first message type that isn't
        referenced by any other ones as our top level message.
        """

        # Instead of using grpc_tool.protoc.main, we directly invoke so that
        # we don't need to specify a bunch of imports.
        subprocess.run(
            [
                sys.executable,  # Use this or can't run pytest properly
                "-m",
                "grpc_tools.protoc",
                f"--proto_path=generated={self.proto_interface_dir.as_posix()}",
                f"--python_out={self.base_dir}",
                file_path.as_posix(),
            ]
        )

        sys.path.append(self.generated_dir.as_posix())
        py_file = self.generated_dir / f"{schema_name.replace('-', '_')}_pb2.py"
        module_name = py_file.stem
        message = importlib.import_module(module_name)
        top_level_message = self._find_top_level_message(message)
        if not top_level_message:
            raise ValueError("Cannot determine top level message")
        class_ = getattr(message, top_level_message)
        instance = class_()
        return instance

    def _find_top_level_message(self, message) -> Optional[str]:
        """
        Go through the entire generated module, and see if we can find a message
        type that isn't referenced by any other message type. If there is such a
        message, we use it as our top level message type.
        """

        file_desc: FileDescriptor = message.DESCRIPTOR

        # Collect message types that are referenced in other messages
        referenced_message_types = set()
        for message in file_desc.message_types_by_name.values():
            for field in message.fields:
                if (
                    ProtobufDataTypes(field.type) is ProtobufDataTypes.TYPE_MESSAGE
                    and field.message_type is not None
                    and str(field.message_type.full_name).startswith(file_desc.package)
                ):
                    referenced_message_types.add(field.message_type.full_name)

        # Find the first message that isn't referenced in other messages
        for message in file_desc.message_types_by_name.values():
            if message.full_name not in referenced_message_types:
                return message.name

        return None  # If no top-level message is found

    def _parse_protobuf_fields(self, fields, cur_path: str) -> List[SchemaField]:
        schema_fields = []
        for field in fields:
            field_path = build_field_path(cur_path, field.name)

            field_type = ProtobufDataTypes(field.type)
            schema_field = SchemaField(
                field_name=field.name,
                field_path=field_path,
                native_type=field_type.name,
                subfields=(
                    self._parse_protobuf_fields(field.message_type.fields, field_path)
                    if field_type is ProtobufDataTypes.TYPE_MESSAGE
                    else None
                ),
            )
            schema_fields.append(schema_field)

        return schema_fields

    def run_parser(self, raw_schema: str, schema_name: str) -> List[SchemaField]:
        file_path = self._create_proto_file(raw_schema, schema_name)
        instance = self._get_protobuf_python_object(file_path, schema_name)
        schema_fields = self._parse_protobuf_fields(
            instance.DESCRIPTOR.fields, cur_path=""
        )

        if Path(self.base_dir).exists():
            shutil.rmtree(self.base_dir)

        return schema_fields

    @staticmethod
    def parse(raw_schema: str, subject: str) -> Optional[List[SchemaField]]:
        try:
            return ProtobufParser().run_parser(raw_schema, subject)
        except Exception:
            logger.exception(f"Failed to parse schema for subject: {subject}")
        return None
