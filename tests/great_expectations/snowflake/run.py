# Import required modules from GX library.
import os
import shutil
from pathlib import Path
from typing import Dict, List, NamedTuple

import great_expectations as gx
import yaml
from great_expectations import core
from openai import BaseModel


def run() -> None:
    class Config(BaseModel):
        user: str
        password: str
        account: str
        warehouse: str
        role: str

        def get_connection_string(self, database: str, schema: str) -> str:
            return f"snowflake://{self.user}:{self.password}@{self.account}/{database}/{schema}?warehouse={self.warehouse}&role={self.role}&application=great_expectations_oss"

    current_path = Path(__file__).parent.resolve()
    config_path = current_path / "config.yml"
    if config_path.exists():
        with open(config_path) as f:
            config = Config.model_validate(yaml.safe_load(f.read()))
    else:
        config = Config(
            user=os.environ["SNOWFLAKE_USER"],
            password=os.environ["SNOWFLAKE_PASSWORD"],
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
            role=os.environ["SNOWFLAKE_ROLE"],
        )

    # Clear gx context.
    if (current_path / "gx").exists():
        shutil.rmtree(current_path / "gx")

    class Dataset(NamedTuple):
        database: str
        schema: str
        table: str

        @property
        def name(self) -> str:
            return f"{self.database}_{self.schema}_{self.table}"

    datasets: Dict[Dataset, list] = {
        Dataset("ACME", "BERLIN_BICYCLES", "CYCLE_HIRE"): [
            gx.expectations.ExpectColumnToExist(column="RENTAL_ID"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="RENTAL_ID"),
            gx.expectations.ExpectColumnValuesToBeNull(column="PRICING_TIER", mostly=0.8),  # type: ignore
            gx.expectations.ExpectColumnValuesToBeDateutilParseable(column="END_DATE"),
        ],
        Dataset("ACME", "BERLIN_BICYCLES", "CYCLE_STATIONS"): [
            gx.expectations.ExpectColumnToExist(column="LATITUDE"),
            gx.expectations.ExpectColumnValuesToBeUnique(column="LATITUDE"),
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="LATITUDE", min_value=51.0, max_value=52.0
            ),
            gx.expectations.ExpectColumnValuesToBeNull(column="PRICING_TIER", mostly=0.8),  # type: ignore
        ],
        Dataset("ACME", "RIDE_SHARE", "CLEANED_BIKE_RIDES"): [
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="TOTAL_MINUTES", min_value=500, max_value=10000, mostly=0.7
            ),
            gx.expectations.ExpectColumnDistinctValuesToBeInSet(
                column="SAME_STATION_FLAG", value_set=[True, False]
            ),
        ],
    }

    # Create Data Context.
    context = gx.get_context(mode="file", project_root_dir=current_path)

    validation_definitions: List[core.validation_definition.ValidationDefinition] = []

    for dataset, expectations in datasets.items():
        database, schema, table = dataset

        # Create Data Source, Data Asset, Batch Definition, and Batch.
        data_source = context.data_sources.add_snowflake(
            f"{dataset.name}-source",
            connection_string=config.get_connection_string(database, schema),
        )
        data_asset = data_source.add_table_asset(name="asset", table_name=table)

        batch_definition = data_asset.add_batch_definition_whole_table(
            "batch_definition"
        )

        # Create Expectation Suite containing two Expectations.
        suite = context.suites.add(
            core.expectation_suite.ExpectationSuite(name=f"{dataset.name}-expectations")
        )
        for expectation in expectations:
            suite.add_expectation(expectation)

        # Create Validation Definition.
        validation_definition = context.validation_definitions.add(
            core.validation_definition.ValidationDefinition(
                name=f"{dataset.name}-validation_definition",
                data=batch_definition,
                suite=suite,
            )
        )

        validation_definitions.append(validation_definition)

    # Create Checkpoint, run Checkpoint, and capture result.
    checkpoint = context.checkpoints.add(
        gx.checkpoint.checkpoint.Checkpoint(
            name="checkpoint", validation_definitions=validation_definitions
        )
    )

    checkpoint.run()


if __name__ == "__main__":
    run()
