import pathlib

from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shared.middleware import spark

DATA_ROOT = pathlib.Path(__file__).parents[1] / "data_store" / "transactions" / "src"
PATH_TO_VALID_DATA = DATA_ROOT / "valid.csv"


# Ideally, you'd store the schema in a schema store. Which could just be another file.
true_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("amt", DecimalType(16, 8), False),
        StructField("from", StringType(), False),
        StructField("to", StringType(), False),
        StructField("dts", TimestampType(), False),
    ]
)

string_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("amt", StringType(), False),  # Changed to string
        StructField("from", StringType(), False),
        StructField("to", StringType(), False),
        StructField("dts", TimestampType(), False),
    ]
)

integer_schema = StructType(
    [
        StructField("id", StringType(), False),
        StructField("amt", DecimalType(16, 8), False),
        StructField("from", IntegerType(), False),  # changed from string to integer
        StructField("to", StringType(), False),
        StructField("dts", TimestampType(), False),
    ]
)

for schema in [true_schema, string_schema, integer_schema]:
    spark.read.schema(schema).csv(path=str(PATH_TO_VALID_DATA), header=True).show(
        n=5, truncate=False
    )
