import datetime
import pathlib
import sys
import uuid

from pyspark.sql.functions import col
from pyspark.sql.functions import min as pymin

from shared.middleware import spark


class QualityException(Exception):
    """Raised when a pipeline does not meet quality standards."""

    pass


try:
    run_arg: str = sys.argv[1]
except IndexError:
    raise IndexError("Pass in a run_id or ISO 8601 compliant date as first argument")

is_date = False
dts_watermark = None
uuid_watermark = None

try:
    dts_watermark = datetime.date.fromisoformat(run_arg)
    is_date = True
except ValueError:
    pass

try:
    uuid_watermark = uuid.UUID(run_arg, version=4)
except ValueError:
    pass

if not any([dts_watermark, uuid_watermark]):
    raise ValueError(
        f"The input argument {sys.argv[1]} cannot be parsed to a date of UUID"
    )


data_store = pathlib.Path(__file__).parents[1] / "data_store"

df = spark.read.format("delta").load(str(data_store / "dq" / "transactions"))

if is_date:
    df = df.where(
        col("__load_dts") >= dts_watermark
    )  # Select the run you want to check
else:
    df = df.where(col("run_id") == str(uuid_watermark))

# easy check
overall_outcome = df.select(pymin("success")).collect()[0][0]

if overall_outcome:
    print("All checks passed")
    sys.exit(0)

# Define custom logic here
df2 = df
df = df.where("result_array.expectation_config.type = 'expect_column_to_exist'")
df = df.where("result_array.expectation_config.kwargs.column in ('id', 'amt', 'from')")
df = df.select(
    col("result_array.success").alias("result"),
    col("result_array.expectation_config.type").alias("check"),
)

df_unique = df2.where(
    "result_array.expectation_config.type = 'expect_column_values_to_be_unique'"
)
df_unique = df_unique.select(
    col("result_array.success").alias("result"),
    col("result_array.expectation_config.type").alias("check"),
)

df = df.unionByName(df_unique)
df.show(truncate=False)

specific_outcome = df.select(pymin("result")).collect()[0][0]

df.where(col("result") == False).show(truncate=False)

failed_checks = df.where(col("result") == False).select("check").collect()  # noqa: E712

if specific_outcome:
    print("Required check(s) passed")
    sys.exit(0)

print(failed_checks)
raise QualityException("Data does not meet quality standards")
