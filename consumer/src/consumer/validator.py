import datetime
import pathlib
import sys
import uuid

from pyspark.sql.functions import col
from pyspark.sql.functions import min as pymin
from quality_gates.middleware import spark

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


data_store = pathlib.Path(__file__).parents[3] / "data_store"

df = spark.read.format("delta").load(str(data_store / "dq" / "transactions"))

if is_date:
    df = df.where(col("load_dts") >= dts_watermark)  # Select the run you want to check
else:
    df = df.where(col("run_id") == str(uuid_watermark))

overall_outcome = df.select(pymin("success")).collect()[0][0]

if overall_outcome:
    sys.exit(0)

# Define custom logic here
