from os.path import basename
from pathlib import Path

from metaphor.common.logger import json_dump_to_debug_file


def test_dump_to_debug_file():
    value = {"foo": "bar"}
    out_file = json_dump_to_debug_file(value, "test")

    assert Path(out_file).read_text() == '{"foo": "bar"}'


def test_dump_to_debug_file_sanitize_file_name():
    value = {"foo": "bar"}
    out_file = json_dump_to_debug_file(value, "illegal/file?name.json")

    assert basename(out_file) == "illegalfilename.json"
