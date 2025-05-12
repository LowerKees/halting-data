"""Demo showing the validation of the written data by a producer."""

import json

import great_expectations as gx
from pyspark.sql.functions import explode

from producer.expectations import transactions
from producer.middleware import data_store, spark
from producer.scanner.schema import result_schema

context = gx.get_context()  # an ephemeral context is enough for the purpose of the demo

# Set up a simple data source since our data lives in in-memory spark data frames
data_source = context.data_sources.add_spark(name="transactions")

# Set up a data asset to bundle findings
asset = data_source.add_dataframe_asset("transaction_data")

# Add a batch which defines how data for the asset is retrieved
batch_def = asset.add_batch_definition_whole_dataframe("transaction_df")


transaction_suite = gx.ExpectationSuite(name="transactions")
context.suites.add(transaction_suite)

for exp in transactions.exps:
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

validation_df = spark.read.schema(result_schema).json(
    spark.sparkContext.parallelize([validation_json])
)

# The nested structure is huge and we only want the success indicator and individual results
validation_df = validation_df.select(
    "success", explode("results").alias("result_array")
)
validation_df.show(truncate=False)

validation_df.write.format("delta").mode("append").save(
    str(data_store / "dq" / "transactions")
)
