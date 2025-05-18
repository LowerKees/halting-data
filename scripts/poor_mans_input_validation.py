"""Shows how to validate user input in Python using a simple function."""

import pathlib

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DecimalType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from shared.middleware import spark


def check_column_names(
    actual_schema: StructType,
    expected_schema: StructType,
    error_on_additional_cols: bool = False,
) -> None:
    print("Checking columns...")
    expected_column_names = [field.name for field in expected_schema]
    actual_column_names = [field.name for field in actual_schema]

    not_found = []
    for expected_column_name in expected_column_names:
        if expected_column_name not in actual_column_names:
            not_found.append(expected_column_name)

    if len(not_found) > 0:
        raise ValueError(f"Expected column name(s) {not_found} not found")

    if error_on_additional_cols:
        print("Checking if all actual columns are in the expected schema")
        check_column_names(
            actual_schema=expected_schema,
            expected_schema=actual_schema,
            error_on_additional_cols=False,
        )


def check_dtype(df: DataFrame, expected_schema: StructType) -> None:
    print("Checking data types...")
    for field in expected_schema:
        c = df.schema[field.name]
        if c.dataType != field.dataType:
            raise TypeError(
                f"Column {c.name} from the data frame is of type {c.dataType} and not "
                + f"of type {field.dataType}"
            )


def check_nullability(df: DataFrame, expected_schema: StructType) -> None:
    print("Checking nullability...")
    for field in expected_schema:
        c = df.schema[field.name]
        if not field.nullable:
            nbr_null = df.where(df[c.name].isNull()).count()

            if nbr_null > 0:
                raise ValueError(
                    f"Non nullable column {c.name} from the data frame containts "
                    + f"{nbr_null} NULL values."
                )


def main():
    schema = StructType(
        [
            StructField("id", StringType(), False),
            StructField("amt", DecimalType(18, 8), False),
            StructField("from", StringType(), False),
            StructField("to", StringType(), False),
            StructField("dts", TimestampType(), False),
        ]
    )
    DATA_ROOT = (
        pathlib.Path(__file__).parents[1] / "data_store" / "transactions" / "gold"
    )

    if not DATA_ROOT.exists():
        raise FileNotFoundError(f"File at path {str(DATA_ROOT)} not found")

    df = spark.read.format("delta").load(path=str(DATA_ROOT), header=True)
    df.show(n=5, truncate=False)

    check_column_names(
        actual_schema=df.schema, expected_schema=schema, error_on_additional_cols=True
    )
    check_dtype(df=df, expected_schema=schema)
    check_nullability(df=df, expected_schema=schema)

    print("Data is valid")


if __name__ == "__main__":
    main()
