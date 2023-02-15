import json
import logging
import tempfile
from typing import Any

_, LOG_FILE = tempfile.mkstemp(suffix=".log")

formatter = logging.Formatter(
    fmt="%(asctime)s:%(levelname)s:%(name)s:%(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)

console = logging.StreamHandler()
console.setLevel(logging.INFO)
console.setFormatter(formatter)

file = logging.FileHandler(LOG_FILE)
file.setLevel(logging.DEBUG)
file.setFormatter(formatter)


def get_logger() -> logging.Logger:
    logger = logging.getLogger("metaphor")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(console)
    logger.addHandler(file)

    return logger


debug_files = []


def add_debug_file(file: str) -> None:
    debug_files.append(file)


def json_dump_to_debug_file(value: Any, file_name: str) -> str:
    out_file = f"{tempfile.mkdtemp()}/{file_name}"
    with open(out_file, "w") as fp:
        fp.write(json.dumps(value, default=str))

    add_debug_file(out_file)
    return out_file
