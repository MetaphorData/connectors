import subprocess
from importlib import import_module

from metaphor.common.cli import parse_args
from metaphor.common.logger import get_logger

logger = get_logger()


def print_packages():
    # Print all pip packages and their versions
    process = subprocess.Popen(
        ["pip", "list"], stdout=subprocess.PIPE, stderr=subprocess.DEVNULL
    )
    for line in process.stdout:
        logger.debug(line.strip().decode())


def main():
    print_packages()

    args = parse_args()

    package_main = getattr(import_module(f"metaphor.{args.name}"), "main", None)
    if package_main is None:
        raise ValueError(f"Unable to load {args.package}:main")
    logger.info(f"Executing {args.name} connector with config file {args.config}")

    package_main(args.config)


if __name__ == "__main__":
    main()
