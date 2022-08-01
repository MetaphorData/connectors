import argparse
from importlib import import_module


def main():
    parser = argparse.ArgumentParser(description="Metaphor Connectors")

    parser.add_argument(
        "name", help="Name of the connector, e.g. snowflake or bigquery"
    )
    parser.add_argument("config", help="Path to the config file")
    args = parser.parse_args()

    package_main = getattr(import_module(f"metaphor.{args.name}"), "main", None)
    if package_main is None:
        raise ValueError(f"Unable to load {args.package}:main")

    package_main(args.config)


if __name__ == "__main__":
    main()
