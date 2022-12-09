from pathlib import Path

from metaphor.common.logger import json_dump_to_debug_file


def test_dump_to_debug_file():
    value = {"foo": "bar"}
    out_file = json_dump_to_debug_file(value, "test")

    assert Path(out_file).read_text() == '{"foo": "bar"}'
