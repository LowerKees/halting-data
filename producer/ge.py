import pathlib

import great_expectations as gx
import great_expectations.expectations as gxe

context = gx.get_context(mode="ephemeral")
source_folder = pathlib.Path(__file__).parents[3] / "data_store" / "transactions"

if not source_folder.exists():
    raise ValueError(f"Directory {str(source_folder)} does not exist")

data_source = context.data_sources.add_spark_filesystem(
    name="transactions", base_directory=source_folder
)

# data_asset_src = data_source.add_directory_delta_asset("src")
data_asset_gold = data_source.add_directory_delta_asset(
    name="gold", data_directory="gold"
)

batch_definition = data_asset_gold.add_batch_definition_whole_dataframe("fullscan")
batch = batch_definition.get_batch(batch_parameters{"dataframe": df})

expectation = gxe.ExpectColumnMaxToBeBetween(max_value=5_000_000)

results = batch.validate(expectation)
