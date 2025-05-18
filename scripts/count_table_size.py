import pathlib

from shared.middleware import spark

DATA = pathlib.Path(__file__).parents[1] / "data_store" / "transactions" / "gold"

df = spark.read.format("delta").load(str(DATA))
print(df.count())

df.show(n=5, truncate=False)
