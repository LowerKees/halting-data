"""Demonstrate the volatility of delta tables"""

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession


def constant_writer(df: DataFrame, output_path: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")  # This changes the whole game
        .save(output_path)
    )


OUTPUT_PATH = "/home/marti/tmp/people"

spark = configure_spark_with_delta_pip(
    SparkSession.builder.appName("DeltaTableSchemaEvolution")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
).getOrCreate()

data = [
    {"id": 1, "name": "John", "age": 30},
    {"id": 2, "name": "Jane", "age": 25},
]

schema = "id INT, name STRING, age INT"

df = spark.createDataFrame(data, schema)

constant_writer(df, output_path=OUTPUT_PATH)

# Read the Delta table
df = spark.read.format("delta").load(OUTPUT_PATH)
df.printSchema()

# Change the schema because we feel like it
data = [
    {"id": 1, "name": "John", "age": 30},
    {"id": 2, "name": "Jane", "age": 25},
]

schema = "id STRING, name STRING, age INT"

df = spark.createDataFrame(data, schema)

constant_writer(df, output_path=OUTPUT_PATH)

# Read the Delta table
df = spark.read.format("delta").load(OUTPUT_PATH)
df.printSchema()
df.show()
