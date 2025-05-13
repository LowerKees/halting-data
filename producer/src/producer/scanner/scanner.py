"""Demo showing the validation of the written data by a producer."""

import json
import pathlib
import uuid

import great_expectations as gx
from pyspark.sql.functions import current_timestamp, explode, lit
from quality_gates.middleware import spark

from producer.expectations import transactions
from producer.scanner.schema import result_schema

data_store = pathlib.Path(__file__).parents[4] / "data_store"

context = gx.get_context()  # an ephemeral context is enough for the purpose of the demo

# Set up a simple data source since our data lives in in-memory spark data frames
data_source = context.data_sources.add_spark(name="transactions")

# Set up a data asset to bundle findings
asset = data_source.add_dataframe_asset("transaction_data")

# Add a batch which defines how data for the asset is retrieved
batch_def = asset.add_batch_definition_whole_dataframe("transaction_df")

transaction_suite = gx.ExpectationSuite(name="transaction")

context.suites.add(transaction_suite)

for exp in transactions.col_exp + transactions.type_exp + transactions.unique_exp:
    transaction_suite.add_expectation(exp)

vd = gx.ValidationDefinition(
    data=batch_def, suite=transaction_suite, name="vd_transaction"
)

context.validation_definitions.add(vd)


df = spark.read.format("delta").load(path=str(data_store / "transactions" / "gold"))
validation_result = vd.run(
    batch_parameters={"dataframe": df}, result_format={"result_format": "SUMMARY"}
)

validation_json = json.dumps(validation_result.to_json_dict())

print("#" * 10)
print(validation_json)
print("#" * 10)

validation_df = spark.read.schema(result_schema).json(
    spark.sparkContext.parallelize([validation_json])
)

# The nested structure is deeply nested and we only want the success indicator and individual results
run_id = uuid.uuid4()
validation_df = (
    validation_df.select("success", explode("results").alias("result_array"))
    .withColumn("run_id", lit(str(run_id)))
    .withColumn("__load_dts", current_timestamp())
)

validation_df.write.format("delta").mode("append").save(
    str(data_store / "dq" / "transactions")
)

print(f"Just finished run with id {str(run_id)}")
