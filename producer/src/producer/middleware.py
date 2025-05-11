import pathlib

from delta import configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession

builder = (
    SparkSession.builder.appName("producer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

_data_store = pathlib.Path(__file__).parents[3] / "data_store"
if not _data_store.exists():
    raise NotADirectoryError(f"Path {str(_data_store)} is not a valid path...")

data_store = _data_store
