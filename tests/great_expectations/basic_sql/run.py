# Import required modules from GX library.
import great_expectations as gx
from great_expectations import core

# Create Data Context.
context = gx.get_context(mode="file")

# Connect to data.
# Create Data Source, Data Asset, Batch Definition, and Batch.
connection_string = "postgresql+psycopg2://try_gx:try_gx@postgres.workshops.greatexpectations.io/gx_example_db"

data_source = context.data_sources.add_postgres(
    "postgres db", connection_string=connection_string
)
data_asset = data_source.add_table_asset(name="taxi data", table_name="nyc_taxi_data")

batch_definition = data_asset.add_batch_definition_whole_table("batch definition")
batch = batch_definition.get_batch()

# Create Expectation Suite containing two Expectations.
suite = context.suites.add(core.expectation_suite.ExpectationSuite(name="expectations"))
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(
        column="passenger_count", min_value=1, max_value=6
    )
)
suite.add_expectation(
    gx.expectations.ExpectColumnValuesToBeBetween(column="fare_amount", min_value=0)
)

# Create Validation Definition.
validation_definition = context.validation_definitions.add(
    core.validation_definition.ValidationDefinition(
        name="validation definition",
        data=batch_definition,
        suite=suite,
    )
)

# Create Checkpoint, run Checkpoint, and capture result.
checkpoint = context.checkpoints.add(
    gx.checkpoint.checkpoint.Checkpoint(
        name="checkpoint", validation_definitions=[validation_definition]
    )
)

checkpoint_result = checkpoint.run()
print(checkpoint_result.describe())
