import pathlib

from shared.middleware import spark

DATA = pathlib.Path(__file__).parents[1] / "data_store" / "dq" / "transactions"
df = spark.read.format("delta").load(str(DATA))

df.show(n=10, truncate=False)
