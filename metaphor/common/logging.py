import logging
import tempfile

LOG_FILE = tempfile.mktemp(suffix=".log")

logging.basicConfig(level=logging.DEBUG, filename=LOG_FILE, filemode="w")

console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger("").addHandler(console)

logging.getLogger(__name__).info(f"Log file: {LOG_FILE}")


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
