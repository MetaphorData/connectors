import logging
import tempfile

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
