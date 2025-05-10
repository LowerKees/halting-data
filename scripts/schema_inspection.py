"""Demonstrate the amount of code you can remove when you inspect your data frames."""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, year
from pyspark.sql.types import DateType, TimestampType


def get_year_boilerplate(df: DataFrame, col_name: str) -> DataFrame:
    """Get the year from the date column"""
    if col_name not in df.columns:
        raise ValueError("DataFrame does not contain a 'date' column")

    if df.schema[col_name].dataType not in [DateType(), TimestampType()]:
        raise TypeError("The 'date' column is not of type date or timestamp")

    return df.withColumn("year", year(col(col_name)))


def get_year(df: DataFrame, col_name: str) -> DataFrame:
    """Get the year from the date column"""
    return df.withColumn("year", year(col(col_name)))


spark = SparkSession.builder.appName("SchemaInspection").getOrCreate()

from datetime import date, datetime

data = [
    (1, date(2023, 1, 1), datetime(2025, 1, 1, 0, 0, 0)),
    (2, date(2023, 2, 1), datetime(2025, 1, 1, 0, 0, 0)),
    (3, date(2023, 3, 1), datetime(2025, 1, 1, 0, 0, 0)),
]

schema = "id INT, date DATE, dts TIMESTAMP"

df = spark.createDataFrame(data, schema)

df = get_year_boilerplate(df, "date")
df.show()
df = get_year_boilerplate(df, "dts")
df.show()
