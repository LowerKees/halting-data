"""Shows how to validate user input in Python using a simple function."""

import pathlib

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

spark = configure_spark_with_delta_pip(
    SparkSession.builder.appName("input_validation")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
).getOrCreate()

DATA_ROOT = pathlib.Path(__file__).parents[1] / "data_store" / "transactions" / "src"
PATH_TO_VALID_DATA = DATA_ROOT / "valid.csv"
PATH_TO_INVALID_DATA = DATA_ROOT / "invalid.csv"

for p in [PATH_TO_VALID_DATA, PATH_TO_INVALID_DATA]:
    if not p.exists():
        raise FileNotFoundError(f"File at path {str(p)} not found")

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


def check_column_names(df: DataFrame, expected_schema: StructType) -> None:
    expected_column_names = [field.name for field in expected_schema]
    actual_column_names = df.columns

    if len(expected_column_names) != len(actual_column_names):
        raise ValueError(
            f"Expected {len(expected_column_names)} columns, "
            + "but got {len(actual_column_names)}"
        )

    expected_column_names.sort()
    actual_column_names.sort()
    for expected, actual in zip(expected_column_names, actual_column_names):
        if expected != actual:
            raise ValueError(f"Expected column name '{expected}', but got '{actual}'")


def check_dtype(df: DataFrame, expected_schema: StructType) -> None:
    for field in expected_schema:
        c = df.schema[field.name]
        if c.dataType != field.dataType:
            raise TypeError(
                f"Column {c.name} from the data frame is of type {c.dataType} and not "
                + f"of type {field.dataType}"
            )


def check_nullability(df: DataFrame, expected_schema: StructType) -> None:
    for field in expected_schema:
        c = df.schema[field.name]
        if not field.nullable:
            nbr_null = df.where(df[c.name].isNull()).count()

            if nbr_null > 0:
                raise ValueError(
                    f"Non nullable column {c.name} from the data frame containts "
                    + f"{nbr_null} NULL values."
                )


# Now, lets do the same for delta tables
DATA_LOCATION = pathlib.Path(__file__).parent / "data" / "valid"

df = spark.read.schema(true_schema).csv(path=str(PATH_TO_VALID_DATA), header=True)
df.write.format("delta").mode("overwrite").save(str(DATA_LOCATION))

df = spark.read.format("delta").load(path=str(DATA_LOCATION))
df.printSchema()
